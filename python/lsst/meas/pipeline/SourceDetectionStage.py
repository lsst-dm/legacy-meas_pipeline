from lsst.pex.harness.Stage import Stage

from lsst.pex.logging import Log

import lsst.pex.policy as policy
import lsst.afw.detection as afwDet
import lsst.afw.image as afwImg
import lsst.afw.math as afwMath
import lsst.pex.exceptions as pexExcept
import lsst.meas.algorithms as measAlg

class SourceDetectionStage(Stage):
    """
    Description:
        This stage wraps the detection of sources on an exposure.
        The exposure to detect should be in the clipboard 
        The key for the exposure can be specified in the policy file. 
        If not specified, default key value will be used.

        The required policy components are detectionPolicy and psfPolicy.
        These include all the necessary parameters to perform the detection,
        and create a smoothing psf 
        
    Policy Dictionaty:
    lsst/meas/pipeline/SourceDetectionStageDictionary.paf

    Clipboard Input:
    - Exposure with key specified by policy attribute exposureKey

    ClipboardOutput:
    - Exposure from input with same key 
    - SmoothingPsf (PSF): the psf used to smooth the exposure before detection 
        Key specified by policy attribute smoothingPsfKey
    - PositiveDetectionSet (DetectionSet): if thresholdPolarity policy 
        is "positive" or "both". Key specified by policy attribute
        positiveDetectionKey
    - NegativeDetectionSet (DetectionSet): if threholdPolarity policy 
        is "negative" or "both". Key specified by policy attribute
        negativeDetectionKey
    """
    def __init__(self, stageId = -1, policy = None):
        # call base constructor
        Stage.__init__(self,stageId, policy)
        # initialize a log
        self.log = Log(Log.getDefaultLog(), 
                "lsst.meas.pipeline.SourceDetectionStage")

    def process(self):
        """
        Detect sources in the worker process
        """
        self.log.log(Log.INFO, "Detecting Sources in process")
        
        self._validatePolicy()               
        clipboard = self.inputQueue.getNextDataset()

        exposure = clipboard.get(self._exposureKey)
        
        dsPositive, dsNegative = self._detectSourcesImpl(exposure)
        self._output(clipboard, dsPositive, dsNegative)
    
    def _detectSourcesImpl(self, exposure): 
        if exposure == None:
            self.log.log(Log.FATAL, 
                "Cannot perform detection - no input exposure")
            raise RuntimeException("No exposure for detection")

        #
        # We need a better way to make a deep copy than this!
        #
        bbox = afwImage.BBox(maskedImage.getXY0(), maskedImage.getWidth(), maskedImage.getHeight())
        exposure = exposure.Factory(exposure, bbox, True) # a deep copy

        try:
            key= self._policy.getString("backgroundSubtractedExposureKey")
        except RuntimeError:
            key = "backgroundSubtractedExposure"
        clipboard.put(key, exposure)

        #
        # Unpack variables
        #
        maskedImage = exposure.getMaskedImage()
        convolvedImage = maskedImage.Factory(maskedImage.getDimensions())
        convolvedImage.setXY0(maskedImage.getXY0())
            
        #
        # Subtract background
        #
        if self._backgroundAlgorithm == "afwMath.NATURAL_SPLINE":
            bctrl = afwMath.BackgroundControl(afwMath.NATURAL_SPLINE)
        else:
            raise RuntimeError, "Unknown backgroundPolicy.algorithm: %s" % (self._backgroundAlgorithm)

        binsize = self._backgroundBinsize

        bctrl.setNxSample(int(maskedImage.getWidth()/binsize) + 1);
        bctrl.setNySample(int(maskedImage.getHeight()/binsize) + 1);
        backobj = afwMath.makeBackground(maskedImage.getImage(), bctrl)

        img = maskedImage.getImage(); img -= backobj.getImageF(); del img

        #
        # Do not propagate the convolved CD/INTRP bits
        # Save them for the original CR/INTRP pixels
        #
        mask = maskedImage.getMask()                
        savedMask = mask.Factory(mask, True)
        savedBits = savedMask.getPlaneBitMask("CR") | \
                    savedMask.getPlaneBitMask("BAD") | \
                    savedMask.getPlaneBitMask("INTRP")
        savedMask &= savedBits
        mask &= ~savedBits;
        del mask
        
        # 
        # Smooth the Image
        #
        self._psf.convolve(convolvedImage, 
                     maskedImage, 
                     convolvedImage.getMask().getMaskPlane("EDGE"))
        
        mask = convolvedImage.getMask()
        mask |= savedMask
        del mask
            
        #
        # Only search psf-smooth part of frame
        #
        llc = afwImg.PointI(self._psf.getKernel().getWidth()/2, 
                            self._psf.getKernel().getHeight()/2)
        urc = afwImg.PointI(convolvedImage.getWidth() - 1,
                            convolvedImage.getHeight() - 1)
        urc -= llc
        bbox = afwImg.BBox(llc, urc)
        middle = convolvedImage.Factory(convolvedImage, bbox)
       
        dsNegative = None 
        if self._negativeThreshold != None:            
            #detect negative sources
            self.log.log(Log.DEBUG, "Do Negative Detection")
            dsNegative = afwDet.makeDetectionSet(middle,
                                                 self._negativeThreshold,
                                                 "DETECTED",
                                                 self._minPixels)
        
        dsPositive = None
        if self._positiveThreshold != None:
            self.log.log(Log.DEBUG, "Do Positive Detection")
            dsPositive = afwDet.makeDetectionSet(maskedImage,
                                                self._positiveThreshold,
                                                "DETECTED",
                                                self._minPixels)
        
        #
        # Reinstate the saved bits in the unsmoothed image
        #
        savedMask <<= convolvedImage.getMask()
        mask = maskedImage.getMask()
        mask |= savedMask

        #
        # clean up
        #
        del middle
        del mask
        del savedMask
       
        return dsPositive, dsNegative
        

    def _output(self, clipboard, dsPositive, dsNegative):
        if dsPositive != None:
            if self._policy.exists("positiveDetectionKey"):
                positiveOutKey = self._policy.getString("positiveDetectionKey")
            else:
                positiveOutKey = "positiveFootprintSet"
            clipboard.put(positiveOutKey, dsPositive)
        
        if dsNegative != None:
            if self._policy.exists("negativeDetectionKey"):
                negativeOutKey = self._policy.getString("negativeDetectionKey")
            else:
                negativeOutKey = "negativeFootprintSet"
            clipboard.put(negativeOutKey, dsNegative)
      
        if self._psf != None:
            clipboard.put(self._smoothingPsfKey, self._psf)

        #and push out the clipboard
        self.outputQueue.addDataset(clipboard)
    
    def _validatePolicy(self):
        """
        Validates the policy object.
        Returns the name of the exposure on the clipboard.
        """
        # Required policy components:
        self._backgroundAlgorithm = self._policy.get("backgroundPolicy.algorithm")
        self._backgroundBinsize = self._policy.get("backgroundPolicy.binsize")

        self._minPixels = self._policy.get("detectionPolicy.minPixels")
        thresholdValue = self._policy.get("detectionPolicy.thresholdValue")
        thresholdType = self._policy.getString("detectionPolicy.thresholdType")
        polarity = self._policy.getString("detectionPolicy.thresholdPolarity")
        
        self._negativeThreshold = None
        if polarity == "negative" or polarity == "both":
            #create a Threshold for negative detections
            self._negativeThreshold = afwDet.createThreshold(thresholdValue,
                                                             thresholdType,
                                                             False)
                                

        self._positiveThreshold = None
        if polarity != "negative":
            #This conditional catches:
            # polarity == "positive"
            # polarity == "both"
            # and malformed polarity values
            # create a Threshold for positive detections
            self._positiveThreshold = afwDet.createThreshold(thresholdValue,
                                                             thresholdType,
                                                             True)
         
        psfPolicy = self._policy.getPolicy("psfPolicy")

        algorithm = psfPolicy.getString("algorithm")
        width = psfPolicy.getInt("width")
        height = psfPolicy.getInt("height")
        if psfPolicy.exists("parameter"):
            parameters = psfPolicy.getDoubleArray("parameter")
            if len(parameters) >= 3:
                self._psf = measAlg.createPSF(algorithm, width, height,
                                              parameters[0],
                                              parameters[1],
                                              parameters[2])
            elif len(parameters) == 2:
                self._psf = measAlg.createPSF(algorithm, width, height,
                                              parameters[0], parameters[1])
            elif len(parameters) == 1:
                self._psf = measAlg.createPSF(algorithm, width, height,
                                               parameters[0])
        else:
            self._psf = measAlg.createPSF(algorithm, width, height)

        self._exposureKey = self._policy.get("exposureKey")
        self._smoothingPsfKey = "psf"
        if self._policy.exists("smoothingPsfKey"):
            self._smoothingPsfKey = self._policy.get("smoothingPsfKey")
