#! /usr/bin/env python

import lsst.pex.harness.stage as harnessStage
from lsst.pex.logging import Log
import lsst.daf.base as dafBase
from lss.daf.base import *
import lsst.pex.policy as pexPolicy

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
        file = pexPolicy.DefaultPolicyFile(meas_pipeline, 
            "MultifitStageDictionary.paf", 
            "pipeline")

        defPolicy = pexPolicy.Policy.createPolicy(file, file.getRepositoryPah())
        if self.policy is None:
            self.policy = defPolicy
        else: 
            self.policy.mergeDefaults(defPolicy)

    def process(self, clipboard):
        pass


