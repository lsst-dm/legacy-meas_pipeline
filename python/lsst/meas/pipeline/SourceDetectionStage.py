import lsst.pex.harness.stage as harnessStage


from lsst.pex.logging import Log

import lsst.pex.policy as pexPolicy
import lsst.afw.detection as afwDet
import lsst.afw.image as afwImg
import lsst.afw.math as afwMath
import lsst.pex.exceptions as pexExcept
import lsst.meas.algorithms as measAlg

import sourceDetection

import lsst.afw.display.ds9 as ds9
import lsst.afw.display.utils as displayUtils

try:
    type(display)
except NameError:
    display = False

class SourceDetectionStageParallel(harnessStage.ParallelProcessing):
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
    - PositiveFootprintSet (DetectionSet): if thresholdPolarity policy 
        is "positive" or "both". Key specified by policy attribute
        positiveDetectionKey
    - NegativeFootprintSet (DetectionSet): if threholdPolarity policy 
        is "negative" or "both". Key specified by policy attribute
        negativeDetectionKey
    """
    def setup(self):
        file = pexPolicy.DefaultPolicyFile("meas_pipeline", 
            "SourceDetectionStageDictionary.paf", "pipeline")
        defPolicy = pexPolicy.Policy.createPolicy(
            file, file.getRepositoryPath())

        if self.policy is None:
            self.policy = defPolicy
        else:
            self.policy.mergeDefaults(defPolicy)

    def process(self, clipboard):
        """
        Detect sources in the worker process
        """
        self.log.log(Log.INFO, "Detecting Sources in process")
        
        #grab exposure from clipboard
        exposure = clipboard.get(self.policy.getString("exposureKey"))
        
        #subtract the background
        backgroundPolicy = self.policy.get("backgroundPolicy")
        backgroundSubtracted, background = sourceDetection.subtractBackground(
            exposure, 
            backgroundPolicy
        )

        #get a smoothing psf
        psf = None
        if self.policy.exists("inputPsfKey"):
            psf = clipboard.get(self.policy.get("inputPsfKey"))
        elif self.policy.exists("psfPolicy"):
            psf = sourceDetection.makePsf(self.policy.get("psfPolicy"))

        #perform detection
        dsPositive, dsNegative = sourceDetection.detectSources(
            backgroundSubtracted, psf, self.policy.get("detectionPolicy"))        

        #output products
        if not dsPositive is None:
            clipboard.put(self.policy.get("positiveDetectionKey"), dsPositive)
        if not dsNegative is None:
            clipboard.put(self.policy.get("negativeDetectionKey"), dsNegative)
        if not background is None:
            clipboard.put(self.policy.get("backgroundKey"), background)
        if not psf is None:
            clipboard.put(self.policy.get("psfKey"), psf)
        clipboard.put(self.policy.get("backgroundSubtractedExposureKey"),
            backgroundSubtracted)
        
        
class SourceDetectionStage(harnessStage.Stage):
    parallelClass = SourceDetectionStageParallel

