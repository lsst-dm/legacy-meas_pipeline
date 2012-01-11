# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#

import math
from math import *

from lsst.pex.logging import Log
import lsst.pex.harness.stage as harnessStage
import lsst.pex.policy as pexPolicy
import lsst.afw.detection

class MultifitFlagIngestStageParallel(harnessStage.ParallelProcessing):
    """
    PT1.2 Hack to ingest status flags from meas_multifit into source
    """
    def setup(self):
        self.log = Log(self.log, "MultifitFlagIngestStage - parallel")

        
        # default policy
        policyFile = pexPolicy.DefaultPolicyFile(\
                "meas_pipeline", 
                "MultifitFlagIngestStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile,
                                                  policyFile.getRepositoryPath(), True)
        
        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy.getDictionary())

    def process(self, clipboard):
        self.log.log(Log.INFO, "MultifitFlagIngestStage is starting")

        statusSchema = lsst.afw.detection.Schema(\
                "flag", self.policy.get("parameters.flagSchemaId"),\
                lsst.afw.detection.Schema.INT)

        #grab sourceSet and apertureCorrection from clipboard
        # correct psf flux in situ
        sourceSet = clipboard.get(self.policy.get("inputKeys.sourceSet"))
        algorithm = self.policy.get("parameters.algorithm")
        for s in sourceSet:
            if(s.getPhotometry()):
                try:
                    photom = s.getPhotometry().find(algorithm)                    
                except:
                    self.log.log(Log.WARN,\
                            "%s measurement not found in photometry for source %d"%\
                            (algorithm, s.getSourceId()))
                    continue

                status = int(photom.get(statusSchema))
                s.setFlagForAssociation(status)
        
class MultifitFlagIngestStage(harnessStage.Stage):
    parallelClass = MultifitFlagIngestStageParallel

