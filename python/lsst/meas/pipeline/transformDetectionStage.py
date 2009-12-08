#! /usr/bin/env python

import lsst.pex.harness.stage as harnessStage
from lsst.pex.logging import Log
import lsst.daf.base as dafBase
from lsst.daf.base import *
import lsst.pex.policy as pexPolicy

class TransformDetectionStageParallel(harnessStage.ParallelProcessing):
    """
    This stage takes a single Model. 
    Computes an ra/dec bounding box for that model.
    Uses the ImageAccessApi to pull down a list of images
      that overlap that bounding box
    For each image in the list, 
        Uses the ImageAccessApi to pull down the exposure's metadata
        computes the model's projection's bbox on that exposure
          using the metadata
        adds the image filename and bbox to a PropertrySet
    puts the property set on the clipboard for future stages to use

    INPUT:
    - a Model
    OUTPUT:
    - a String representing the image filename-bbox pairs used to input
      an ExposureStack
    """
    
    def setup(self):
        self.log = Log(self.log, "TransformDetectionStage - parallel")
 
        policyFile = pexPolicy.DefaultPolicyFile("meas_pipeline", 
            "TransformDetectionStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath(), True)

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy.getDictionary())

    def process(self, clipboard):
        pass


