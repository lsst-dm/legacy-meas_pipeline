#! python
import lsst.pex.harness.stage as harnessStage

from lsst.pex.logging import Log

import lsst.pex.policy as pexPolicy
import lsst.afw.detection as afwDet
import lsst.afw.image as afwImg
import lsst.afw.math as afwMath
import lsst.pex.exceptions as pexExcept
import lsst.meas.algorithms as measAlg

import lsst.meas.utils.sourceDetection as sourceDetection

import lsst.afw.display.ds9 as ds9
import lsst.afw.display.utils as displayUtils

try:
    type(display)
except NameError:
    display = False

class BackgroundEstimationStageParallel(harnessStage.ParallelProcessing):
    """
    Description:
        This stage wraps estimating and possibly subtracting the background from an exposure
        on the clipboard.        

    Policy Dictionary:
    lsst/meas/pipeline/policy/BackgroundEstimationStageDictionary.paf

    Clipboard Input:
    - Calibrated science Exposure(s) (including background)
    - a PSF may be specified by policy attribute inputPsfKey. Alternatively, the
      stage's policy may request that a psf be constructed, by providing the
      psfPolicy attribute.

    ClipboardOutput:
    - background subtracted Exposure used in the detection. Key specified
        by policy attribute 'backgroundSubtractedExposureKey'
    - the measured background object itself. Key specified by policy 
        attribute 'background'        
    - PSF: the psf used to smooth the exposure before detection 
        Key specified by policy attribute 'psfKey'
    - PositiveFootprintSet (DetectionSet): if thresholdPolarity policy 
        is "positive" or "both". Key specified by policy attribute
        'positiveDetectionKey'
    - NegativeFootprintSet (DetectionSet): if threholdPolarity policy 
        is "negative" or "both". Key specified by policy attribute
        'negativeDetectionKey'
    """
    def setup(self):
        self.log = Log(self.log, "BackgroundEstimationStage - parallel")

        policyFile = pexPolicy.DefaultPolicyFile("meas_pipeline", 
            "BackgroundEstimationStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath(), True)

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy)

    def process(self, clipboard):
        """
        Detect sources in the worker process
        """
        self.log.log(Log.INFO, "Subtracting background in process")
        
        #grab exposure from clipboard
        exposure = clipboard.get(self.policy.getString("inputKeys.exposureKey"))
            
        #subtract the background
        backgroundSubtracted, background = sourceDetection.subtractBackground(
            exposure, self.policy.get("backgroundPolicy"))

        #output products
        clipboard.put(self.policy.get("outputKeys.backgroundKey"), background)
        clipboard.put(self.policy.get("outputKeys.backgroundSubtractedExposureKey"), backgroundSubtracted)
        
        
class BackgroundEstimationStage(harnessStage.Stage):
    parallelClass = BackgroundEstimationStageParallel

