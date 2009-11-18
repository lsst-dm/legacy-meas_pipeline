#! /usr/bin/env python

import lsst.pex.harness.stage as harnessStage
from lsst.pex.logging import Log
import lsst.daf.base as dafBase
from lsst.daf.base import *
import lsst.pex.policy as pexPolicy

__all__ = ["MultifitStage", "MultifitStageParallel"]

class MultifitStageParallel(harnessStage.ParallelProcessing):
    """
    Given an Exposure Stack and an initial Model, fit the model on the
    stack using multifit.

    INPUT:
    - a Model
    - an ExposureStack
    OUTPUT:
    - a Model
    - a chisq
    - a covariance matrix

    """
    
    def setup(self):
        self.log = Log(self.log, "MultifitStage - parallel")
        policyFile = pexPolicy.DefaultPolicyFile("meas_pipeline", 
            "MultifitStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, 
            policyFile.getRepositoryPah())

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy)

    def process(self, clipboard):
        pass

class MultifitStage(harnessStage.Stage):
    parallelClass = MultifitStageParallel
