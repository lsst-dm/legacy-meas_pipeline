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
    def process(self):
        log = Log(Log.getDefaultLog(), 
            "lsst.meas.pipeline.SourceDetectionStage")
        log.log(Log.INFO, "Detecting Sources in process")
        
        clipboard = self.inputQueue.getNextDataset()

        exposure = clipboard.get(self._policy.get("exposureKey"))
        exposure = measAlg.makeBackgroundSubtractedExposure(
                exposure,
                self._policy.get("backgroundPolicy"))
        #
        # Write the background-subtracted exposure out to the clipboard
        # 
        if self._policy.exists("backgroundSubtractedExposureKey"):
            key = self._policy.getString("backgroundSubtractedExposureKey")
            clipboard.put(key, exposure)
        
        if display:
            maskedImage = exposure.getMaskedImage()
            ds9.mtv(maskedImage)
            del maskedImage

        if not ds9.getMaskPlaneColor("DETECTED_NEGATIVE"):
            ds9.setMaskPlaneColor("DETECTED_NEGATIVE", ds9.CYAN)

        try:
            psf = self._getOrMakePsf(clipboard)
        except RuntimeError:
            log.log(Log.FATAL, "Source detection failed: missing a PSF")
            return
            

        dsPositive, dsNegative = measAlg.detectSources(
                exposure, 
                psf, 
                self._policy.get("detectionPolicy"))

        self._output(clipboard, dsPositive, dsNegative, psf)


    def _output(self, clipboard, dsPositive, dsNegative, psf):
        #
        # output the detection sets to the clipboard
        # 
        if self._policy.exists("positiveDetectionKey") and dsPositive != None:
                positiveOutKey = self._policy.getString("positiveDetectionKey")
                clipboard.put(positiveOutKey, dsPositive)
        
        if self._policy.exists("negativeDetectionKey") and dsNegative != None:
                negativeOutKey = self._policy.getString("negativeDetectionKey")
                clipboard.put(negativeOutKey, dsNegative)
    
        #
        # Output the psf used out to clipboard
        #
        clipboard.put(self._policy.get("psfKey"), psf)

        #and push out the clipboard
        self.outputQueue.addDataset(clipboard)
    
    def _getOrMakePsf(self, clipboard):
        if self._policy.exists("inputPsfKey"):
            psfKey = self._policy.get("inputPsfKey")
            psf = clipboard.get(psfKey)
            if psf != None:
                return psf

        if not self._policy.exists("psfPolicy"):
            raise RuntimeError("SourceDetection Failed: missing a PSF")

        psfPolicy = self._policy.getPolicy("psfPolicy")
        params = []        
        params.append(psfPolicy.getString("algorithm"))
        params.append(psfPolicy.getInt("width"))
        params.append(psfPolicy.getInt("height"))
        if psfPolicy.exists("parameter"):
            params += psfPolicy.getDoubleArray("parameter")
        
        return measAlg.createPSF(*params)
