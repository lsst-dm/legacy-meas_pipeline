from lsst.pex.harness.Stage import Stage

from lsst.pex.logging import Log

import lsst.pex.policy as policy
import lsst.afw.detection as afwDet
import lsst.afw.image as afwImg
import lsst.afw.math as afwMath
import lsst.pex.exceptions as pexExcept
import lsst.meas.algorithms as measAlg

import lsst.afw.display.ds9 as ds9
import lsst.afw.display.utils as displayUtils

try:
    type(display)
except NameError:
    display = False

class SourceDetectionStage(Stage):
    """
    Description:
        This stage wraps the detection of sources on an exposure.
        The exposure to detect should be in the clipboard 
        The key for the exposure can be specified in the policy file. 
        If not specified, default key value will be used.

    Policy Dictionaty:
    lsst/meas/pipeline/SourceDetectionStageDictionary.paf

    Clipboard Input:
    - Exposure with key specified by policy attribute exposureKey
    - optionally a PSF may be specified by policy attribute psfKey

    ClipboardOutput:
    - Exposure from input with same key 
    - PSF: the psf used to smooth the exposure before detection 
        Key specified by policy attribute psfKey
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
        exposure = self._makeBackgroundSubtractedExposure(exposure) 

        psf = self._getOrMakePsf(clipboard)
        dsPositive, dsNegative = self._detectSourcesImpl(exposure, psf)
        
        self._output(clipboard, dsPositive, dsNegative, exposure, psf)
   
    def _makeBackgroundSubtractedExposure(self, exposure):
        if self._backgroundAlgorithm == None:
            return exposure

        maskedImage = exposure.getMaskedImage()
        #make a deep copy
        deepCopy = maskedImage.Factory(maskedImage, True)
        deepCopy.setXY0(maskedImage.getXY0())
      
        #
        # Subtract background
        #
        if self._backgroundAlgorithm == "NATURAL_SPLINE":
            bctrl = afwMath.BackgroundControl(afwMath.NATURAL_SPLINE)
        else:
            raise RuntimeError, "Unknown backgroundPolicy.algorithm: %s" % \
                    (self._backgroundAlgorithm)

        binsize = self._backgroundBinsize

        bctrl.setNxSample(int(deepCopy.getWidth()/binsize) + 1)
        bctrl.setNySample(int(deepCopy.getHeight()/binsize) + 1)
        backobj = afwMath.makeBackground(deepCopy.getImage(), bctrl)

        image = deepCopy.getImage() 
        image -= backobj.getImageF()
        del image

        return exposure.Factory(deepCopy, afwImg.Wcs(exposure.getWcs()))
    
    def _detectSourcesImpl(self, exposure, psf): 
        if exposure == None:
            self.log.log(Log.FATAL, 
                "Cannot perform detection - no input exposure")
            raise RuntimeException("No exposure for detection")

        #
        # Unpack variables
        #
        maskedImage = exposure.getMaskedImage()
        convolvedImage = maskedImage.Factory(maskedImage.getDimensions())
        convolvedImage.setXY0(maskedImage.getXY0())

        if display:
            ds9.mtv(maskedImage)
            
        # 
        # Smooth the Image
        #
        psf.convolve(convolvedImage, 
                           maskedImage, 
                           convolvedImage.getMask().getMaskPlane("EDGE"))
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
       
        dsNegative = None 
        if self._negativeThreshold != None:            
            #detect negative sources
            self.log.log(Log.DEBUG, "Do Negative Detection")
            dsNegative = afwDet.makeDetectionSet(middle,
                                                 self._negativeThreshold,
                                                 "DETECTED_NEGATIVE",
                                                 self._minPixels)
            if not ds9.getMaskPlaneColor("DETECTED_NEGATIVE"):
                ds9.setMaskPlaneColor("DETECTED_NEGATIVE", ds9.CYAN)
        
        dsPositive = None
        if self._positiveThreshold != None:
            self.log.log(Log.DEBUG, "Do Positive Detection")
            dsPositive = afwDet.makeDetectionSet(middle,
                                                self._positiveThreshold,
                                                "DETECTED",
                                                self._minPixels)
        #
        # ds only searched the middle but it belongs to the entire MaskedImage
        #
        dsPositive.setRegion(afwImg.BBox(afwImg.PointI(maskedImage.getX0(), maskedImage.getY0()),
                                           maskedImage.getWidth(), maskedImage.getHeight()));
        if dsNegative:
            dsNegative.setRegion(afwImg.BBox(afwImg.PointI(maskedImage.getX0(), maskedImage.getY0()),
                                             maskedImage.getWidth(), maskedImage.getHeight()));
        #
        # We want to grow the detections into the edge by at least one pixel so that it sees the EDGE bit
        #
        grow, isotropic = 1, False
        dsPositive = afwDet.DetectionSetF(dsPositive, grow, isotropic)
        dsPositive.setMask(maskedImage.getMask(), "DETECTED")

        if dsNegative:
            dsNegative = afwDet.DetectionSetF(dsNegative, grow, isotropic)
            dsNegative.setMask(maskedImage.getMask(), "DETECTED_NEGATIVE")
        #
        # clean up
        #
        del middle

        return dsPositive, dsNegative

    def _output(self, clipboard, dsPositive, dsNegative, exposure, psf):
        if dsPositive != None:
            if self._policy.exists("positiveDetectionKey"):
                positiveOutKey = self._policy.getString("positiveDetectionKey")
                clipboard.put(positiveOutKey, dsPositive)
        
        if dsNegative != None:
            if self._policy.exists("negativeDetectionKey"):
                negativeOutKey = self._policy.getString("negativeDetectionKey")
                clipboard.put(negativeOutKey, dsNegative)
    
        if self._backgroundAlgorithm != None and exposure != None:
            if self._policy.exists("backgroundSubtractedExposureKey"):
                key = self._policy.getString("backgroundSubtractedExposureKey")
                clipboard.put(key, exposure)

        clipboard.put(self._policy.get("psfKey"), psf)

        #and push out the clipboard
        self.outputQueue.addDataset(clipboard)
    
    def _validatePolicy(self):
        """
        Validates the policy object.
        Returns the name of the exposure on the clipboard.
        """
        if self._policy.exists("backgroundPolicy"):
            self._backgroundAlgorithm = \
                    self._policy.get("backgroundPolicy.algorithm")
            self._backgroundBinsize = \
                    self._policy.get("backgroundPolicy.binsize")
        else:
            self._backgroundAlgorithm = None

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
        
        self._exposureKey = self._policy.get("exposureKey")
        
    def _getOrMakePsf(self, clipboard):
        if self._policy.exists("inputPsfKey"):
            psfKey = self._policy.get("inputPsfKey")
            psf = clipboard.get(psfKey)
            if psf != None:
                return psf
            else:
                self.log.log(Log.WARN,
                        "inputPsfKey %s not found on clipboard"%psfKey)

        psfPolicy = self._policy.getPolicy("psfPolicy")
        params = []        
        params.append(psfPolicy.getString("algorithm"))
        params.append(psfPolicy.getInt("width"))
        params.append(psfPolicy.getInt("height"))
        if psfPolicy.exists("parameter"):
            params += psfPolicy.getDoubleArray("parameter")
        
        return measAlg.createPSF(*params)
