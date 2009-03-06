from lsst.pex.harness.Stage import Stage

import lsst.pex.harness.Utils
from lsst.pex.logging import Log

import lsst.daf.base as dafBase
from lsst.daf.base import *

import lsst.afw.detection as afwDet
import lsst.afw.image as afwImg


class SourceDetectionStage(Stage):
    """
    Description:
        This stage wraps the detection of sources on an exposure.
        The exposure to detect should be in the clipboard along with a psf
        to use in smoothing the image. The key for the exposure and the psf
        can be specified in the policy file. If not specified, default key 
        values will be used.

        The required policy components are thresholValue (which specifies
        the threshold for detection) and minPixelsPerSource (which specifies
        the minimum size of sources to consider in number of pixels). 
        
        See below for optional policy components.

    Policy Input: 
    (see detection/pipeline/SourceDetectionStageDictionary.paf)
    - thresholdValue (double): REQUIRED specify threshold value
    - minPixelsPerSource (int): REQUIRED specify minimum source size
    - runMode (string): optional, default "process"    
    - exposureKey (string): optional, default "Exposure"
    - psfKey (string): optional, default "PSF"
    - exposurePixelType (string): optional, default "float"
    - thresholdType (string): optional, default "value"
    - thresholdPolarity (string): optional, default "positive"

    Clipboard Input:
    - Exposure with key name specified in policy ("exposureName")
    - Psf with key name specified in policy ("psfKey")

    ClipboardOutput:
    - Exposure from input with same key name
    - Psf from input with same key name
    - PositiveDetectionSet (DetectionSet): if thresholdPolarity policy 
        is "positive" or "both"
    - NegativeDetectionSet (DetectionSet): if threholdPolarity policy 
        is "negative" or "both"
    """

    def __init__(self, stageId = -1, policy = None):
        # call base constructor
        lsst.pex.harness.Stage.Stage.__init__(self,stageId, policy)
        # initialize a log
        self.log = Log(Log.getDefaultLog(), 
                "lsst.meas.pipeline.SourceDetectionStage")

    def preprocess(self):
        """
        Detect sources in the master process before any processing
        """
        if self._policy.exists('runMode') and \
                self._policy.getString('runMode') == 'preprocess':
            self.log.log(Log.INFO, "Detecting Sources in preprocess")
            self._detectSources()
        

    def process(self):
        """
        Detect sources in the worker process
        """
        if not self._policy.exists('runMode') or \
                self._policy.getString('runMode') == 'process':
            self.log.log(Log.INFO, "Detecting Sources in process")
            self._detectSources()

    
    def postprocess(self):
        """
        Detect sources in the master process after any processing
        """
        if self._policy.exists('runMode') and \
                self._policy.getString('runMode') == 'postprocess':
            self.log.log(Log.INFO, "Detecting Sources in postprocess")
            self._detectSources()


    def _detectSources(self):
        self._validatePolicy()
        
        queueLength = self.inputQueue.size()       
        self.log.log(Log.INFO, ("%d Datasets found in inputQueue" % queueLength))

        for i in xrange(queueLength):
            clipboard = self.inputQueue.getNextDataset()
            exposure = clipboard.get(self._exposureName)

            if exposure == None:
                self.log.log(Log.WARN, "No input exposure - skipping dataset")
                continue

            psf = clipboard.get(self._psfName)
            if psf == None:
                self.log.log(Log.WARN, "No input psf - skipping dataset")
                continue

            maskedImage = exposure.getMaskedImage()
            convoledImage = maskedImage.Factory(maskedImage.getDimensions())
            origin = afwImg.PointI(maskedImage.getX0(), maskedImage.getY0())
            convolvedImage.setXY0(origin)
            
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
            psf.convolve(convolvedImage, 
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
                dsNegative = self._detectionSetType(middle,
                                                    self._negativeThreshold,
                                                    "FP-",
                                                    self._minPixels)
                clipboard.put("NegativeDetectionSet", dsPositive)

            if self._positiveThreshold != None:            
                #detect positive sources
                dsPositive = self._detectionSetType(maskedImage,
                                                    self._positiveThreshold,
                                                    "FP+",
                                                    self._minPixels)
                clipboard.put("PositiveDetectionSet", dsPositive)

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

            # 
            # Push clipboard to outputQueue
            #
            self.outputQueue.addDataSet(clipboard)


    def _validatePolicy(self):
        """
        Validates the policy object.
        Returns the name of the exposure on the clipboard.
        """
        #Required policy components:
        # minPixelsPerSource: defines the minimum source size
        # thresholdValue: defines the detection threshold
        self.minPixels = self._policy.getInt("minPixelsPerSource")
        thresholdValue = self._policy.getPolicy("thresholdValue")

        if not self._policy.exists("exposurePixelType"):
            self.log.log(Log.WARN, "Using default \
                    exposurePixelType=\"float\"")    
            self._detectionSetType = afwDet.DetectionSetF
        else:
            exposurePixelType = self.policy.getString("exposurePixelType")
            if exposurePixelType == "int":
                self._detectionSetType = afwDet.DetectionSetI
            elif exposurePixelType == "double":
                self._detectionSetType = afwDet.DetectionSetD
            else:
                if exposurePixelType != "float":
                    self.log.log(Log.WARN, ("Illegal pixel type: %s \
                            specified for policy component exposurePixelType"\
                            % exposurePixelType))
                    self.log.log(Log.WARN, "Using default \
                            exposurePixelType=\"float\"")                                          
                
                self._detectionSetType = afwDet.DetectionSetF

        #Look for a threshold type
        #Default to "value"
        if not self._policy.exists("thresholdType"):
            thresholdType = "value"
            self.log.log(Log.WARN, "Using default \
                    thresholdType=\"value\"")
        else:
            thresholdType = self._policy.getString("thresholdType")
            
        #Look for a threshold polarity.
        #Default to positive
        if not self._policy.exists("thresholdPolarity"):
            polarity = "positive"
            self.log.log(Log.WARN, "Using default \
                    thresholdPolarity=\"positive\"")
        else:
            polarity = self._policy.getString("thresholdPolarity")

        self._negativeThreshold = None
        if polarity == "negative" or polarity == "both":
            #create a Threshold for negative detections
            policy.set("polarity", False)
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
            policy.set("polarity", True)
            self._positiveThreshold = afwDet.createThreshold(thresholdValue,\
                                                            thresholdType,\
                                                            True)

        if self._policy.exists("exposureKey") 
            self._exposureKey = self._policy.getString("exposureKey")
        else:
            self.log.log(Log.WARN, "Using default exposureKey=\"Exposure\"")      
            self._exposureKey = "Exposure"

        if self._policy.exists("psfKey"):
            self._psfKey = self._policy.getString("psfKey")
        else:
            self.log.log(Log.WARN, "Using default psfKey=\"PSF\"")
            self._psfKey = "PSF"
