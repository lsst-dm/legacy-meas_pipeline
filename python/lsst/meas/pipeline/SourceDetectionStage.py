from lsst.pex.harness.Stage import Stage

from lsst.pex.logging import Log

import lsst.afw.detection as afwDet
import lsst.afw.image as afwImg
import lsst.pex.exceptions as pexExcept

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
    - PositiveDetectionSet (DetectionSet): if thresholdPolarity policy 
        is "positive" or "both". Key specified by policy attribute
        positiveDetectionKey
    - NegativeDetectionSet (DetectionSet): if threholdPolarity policy 
        is "negative" or "both". Key specified by policy attribute
        negativeDetectionKey
    """
    def __init__(self, stageId = -1, policy = None):
        # call base constructor
        lsst.pex.harness.Stage.Stage.__init__(self,stageId, policy)
        # initialize a log
        self.log = Log(Log.getDefaultLog(), 
                "lsst.meas.pipeline.SourceDetectionStage")

    def process(self):
        """
        Detect sources in the worker process
        """
        self.log.log(Log.INFO, "Detecting Sources in process")
        self.detectSources()

    
    def detectSources(self):
        self._validatePolicy()               
        
        clipboard = self.inputQueue.getNextDataset()

        self.log.log(Log.DEBUG, "getting exposure from clipboard")
        exposure = clipboard.get(self._exposureKey)
        
        dsPositive, dsNegative = self._detectSourcesImpl(exposure)
        self._output(clipboard, dsPositive, dsNegative)
    
    def _detectSourcesImpl(self, exposure): 
        if exposure == None:
            self.log.log(Log.FATAL, 
                "Cannot perform detection - no input exposure")
            raise pexExcept.NotFoundException("No exposure for detection")
        
        maskedImage = getMaskedImage()
        convoledImage = maskedImage.Factory(maskedImage.getDimensions())
        convolvedImage.setXY0(maskedImage.getXY0())
            
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
        _psf.convolve(convolvedImage, 
                     maskedImage, 
                     convolvedImage.getMask().getMaskPlane("EDGE"))
        
        mask = convolvedImage.getMask()
        mask |= savedMask
        del mask
            
        #
        # Only search psf-smooth part of frame
        #
        llc = afwImg.PointI(psf.getKernel().getWidth()/2, 
                            psf.getKernel().getHeight()/2)
        urc = afwImg.PointI(convolvedImage.getWidth() - 1,
                            convolvedImage.getHeight() - 1)
        urc -= llc
        bbox = afwImg.BBox(llc, urc)
        middle = convolvedImage.Factory(convolvedImage, bbox)
        
        if self._negativeThreshold != None:            
            #detect negative sources
            dsNegative = afwDet.makeDetectionSet(middle,
                                                 self._negativeThreshold,
                                                 "FP-",
                                                 self._minPixels)
        
        if self._positiveThreshold != None:
            #detect positive sources
            dsPositive = afwDet.makeDetectionSet(maskedImage,
                                                self._positiveThreshold,
                                                "FP+",
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
                positiveOutKey = "PositiveFootprintSet"
            clipboard.put(positiveOutKey, dsPositive)
        
        if dsNegative != None:
            if self._policy.exists("negativeDetectionKey"):
                negativeOutKey = self._policy.getString("negativeDetectionKey")
            else:
                negativeOutKey = "NegativeFootprintSet"
            clipboard.put(negativeOutKey, dsNegative)
        
        #and push out the clipboard
        self.outputQueue.addDataSet(clipboard)
    
    def _validatePolicy(self):
        """
        Validates the policy object.
        Returns the name of the exposure on the clipboard.
        """
        # Required policy components:
        self._minPixels = self._policy.get("detectionPolicy.minPixels")
        thresholdValue = self._policy.get("detectionPlicy.thresholdValue")
        
        #Default to "value"
        thresholdType = self._policy.get("detectionPolicy.thresholdType")
         
        #Default to positive
        polarity = self._policy.get("detectionPolicy.thresholdPolarity")
        
        self._negativeThreshold = None
        if polarity == "negative" or polarity == "both":
            #create a Threshold for negative detections
            self._negativeThreshold = afwDet.createThreshold(thresholdValue,\
                                                             thresholdType,\
                                                             False)
        
        self._positiveThreshold = None
        if polarity != "negative":
            #This conditional catches:
            # polarity == "positive"
            # polarity == "both"
            # and malformed polarity values
            # create a Threshold for positive detections
            self._positiveThreshold = afwDet.createThreshold(thresholdValue,\
                                                             thresholdType,\
                                                             True)
         
        psfPolicy = self._policy.getPolicy("psfPolicy")

        args= []
        args.append(psfPolicy.getString("algorithm"))
        args.append(psfPolicy.getInt("width"))
        args.append(psfPolicy.getInt("height"))
        if psfPolicy.exists("parameter"):
            parameters = psfPolicy.getDoubleArray("parameter")
            for param in parameters:
                args.append(param)

        self._psf = measAlg.createPsf(args)

        self._exposureKey = self._policy.getString("exposureKey")
