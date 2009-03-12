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
    - psfPolicy (policy):optional, default lsst/meas/algorithms/pipeline/PSF.paf
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
            self.detectSources()
        

    def process(self):
        """
        Detect sources in the worker process
        """
        if not self._policy.exists('runMode') or \
                self._policy.getString('runMode') == 'process':
            self.log.log(Log.INFO, "Detecting Sources in process")
            self.detectSources()

    
    def postprocess(self):
        """
        Detect sources in the master process after any processing
        """
        if self._policy.exists('runMode') and \
                self._policy.getString('runMode') == 'postprocess':
            self.log.log(Log.INFO, "Detecting Sources in postprocess")
            self.detectSources()


    def detectSources(self):
        self.__validatePolicy__()               
        
        clipboard = self.inputQueue.getNextDataset()
        exposure = clipboard.get(self.__exposureKey__)
       
        dsPositive, dsNegative = self.__detectSourcesImpl()
        self.__output__(clipboard, dsPositive, dsNegative)
    
    def __detectSourcesImpl__(self, exposure): 
        if exposure == None:
            self.log.log(Log.FATAL, 
                    "Cannot perform detection - no input exposure")
            return 
        
         
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
        __psf__.convolve(convolvedImage, 
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
        
        if self.__negativeThreshold__ != None:            
            #detect negative sources
            dsNegative = afwDet.makeDetectionSet(middle,
                                                 self.__negativeThreshold__,
                                                 "FP-",
                                                 self.__minPixels__)
        
        if self.__positiveThreshold__ != None:
            #detect positive sources
            dsPositive = afwDet.makeDetectionSet(maskedImage,
                                                self.__positiveThreshold__,
                                                "FP+",
                                                self.__minPixels__)
        
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
        

    def __output__(self, clipboard, dsPositive, dsNegative):
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
    
    def __validatePolicy__(self):
        """
        Validates the policy object.
        Returns the name of the exposure on the clipboard.
        """
        # Required policy components:
        # minPixelsPerSource: defines the minimum source size
        # thresholdValue: defines the detection threshold
        self.__minPixels__ = self._policy.get("detectionPolicy.minPixels")
        thresholdValue = self._policy.get("detectionPlicy.thresholdValue")
        
        #Default to "value"
        thresholdType = self._policy.get("detectionPolicy.thresholdType")
         
        #Default to positive
        polarity = self._policy.get("detectionPolicy.thresholdPolarity")
        
        self.__negativeThreshold__ = None
        if polarity == "negative" or polarity == "both":
            #create a Threshold for negative detections
            self.__negativeThreshold__ = afwDet.createThreshold(thresholdValue,\
                                                             thresholdType,\
                                                             False)
        
        self.__positiveThreshold__ = None
        if polarity != "negative":
            #This conditional catches:
            # polarity == "positive"
            # polarity == "both"
            # and malformed polarity values
            # create a Threshold for positive detections
            self.__positiveThreshold__ = afwDet.createThreshold(thresholdValue,\
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

        self.__psf__ = measAlg.createPsf(args)

        self.__exposureKey__ = self._policy.getString("exposureKey")
