from lsst.pex.logging import Log
import lsst.pex.harness.stage as harnessStage
import lsst.pex.policy as pexPolicy

__all__ = ["ForcedPhotometryStage", "ForcedPhotometryStageParallel"]

class ForcedPhotometryStageParallel(harnessStage.ParallelProcessing):
    """
    Given model(s) for an object, measure the source on the exposures
    provided in the ExposureStack

    Clipboard input:
    - Model(s): point source and/or small galaxy model    
    - ExposureStack
    """

    def setup(self):
        self.log = Log(self.log, "ForcedPhotometryStage - parallel")

        policyFile = pexPolicy.DefaultPolicyFile("meas_pipeline", 
            "ForcedPhotometryStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, 
            policyFile.getRepositoryPath())

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy)

    def process(self, clipboard):
        pass

class ForcedPhotometryStage(harnessStage.Stage):
    parallelClass = ForcedPhotometryStageParallel
