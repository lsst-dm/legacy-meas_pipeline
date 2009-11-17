import math
from math import *

from lsst.pex.logging import Log
import lsst.pex.harness.stage as harnessStage
import lsst.pex.policy as pexPolicy
import lsst.meas.algorithms as measAlg
import lsst.meas.algorithms.Psf as Psf
import lsst.sdqa as sdqa

class PsfDeterminationStageParallel(harnessStage.ParallelProcessing):
    """
    Given an exposure and a set of sources measured on that exposure,
    determine a PSF for that exposure.

    This stage works on lists of (exposure, sourceSet) pairs.

    Their location on the clipboard is specified via policy.
    see lsst/meas/pipeline/pipeline/PsfDeterminationStageDictionary.paf
    for details on configuring valid stage policies
    """
    def setup(self):
        file = pexPolicy.DefaultPolicyFile("meas_pipeline", 
            "PsfDeterminationStageDictionary.paf", "pipeline")
        defPolicy = pexPolicy.Policy.createPolicy(file, 
            file.getRepositoryPath())

        if self.policy is None:
            self.policy = defPolicy
        else:
            self.policy.mergeDefaults(defPolicy)

    def process(self, clipboard):
        dataList = []
        dataPolicyList = self.policy.getPolicyArray("data")

        sdqaRatings = sdqa.SdqaRatingSet()
        for dataPolicy in dataPolicyList:
            exposureKey = dataPolicy.getString("exposureKey")
            exposure = clipboard.get(exposureKey)
            if exposure is None:
                self.log.log(Log.FATAL, 
                    "No Exposure with key " + exposureKey)
                continue
            sourceSetKey = dataPolicy.getString("sourceSetKey")
            sourceSet = clipboard.get(sourceSetKey)
            if sourceSet is None:
                self.log.log(Log.FATAL, 
                    "No SourceSet with key " + sourceSetKey)
                continue

            psfOutKey = dataPolicy.getString("outputPsfKey")
            outputCellSetKey = dataPolicy.getString("outputCellSetKey")
            psf, cellSet = Psf.getPsf(exposure, sourceList, 
                self.policy, sdqaRatings)

            clipboard.put(outKey, psf)
            clipboard.put(outputCellSetKey, cellSet)


        sdqaKey = self.policy.getString("sdqaKey")
        clipboard.put(sdqaKey, sdqa.PersistableSdqaRatingVector(sdqaRatings))

class PsfDeterminationStage(harnessStage.Stage):
    parallelClass = PsfDeterminationStageParallel

