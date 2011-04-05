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
import lsst.meas.algorithms as measAlg
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
        self.log = Log(self.log, "PsfDeterminationStage - parallel")

        policyFile = pexPolicy.DefaultPolicyFile("meas_pipeline", 
                                                 "PsfDeterminationStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath(), True)
        
        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy.getDictionary())

        starSelectorName = self.policy.get("starSelectorName")
        starSelectorPolicy = self.policy.getPolicy("starSelectorPolicy")
        self.starSelector = measAlg.makeStarSelector(starSelectorName, starSelectorPolicy)

        psfDeterminerName = self.policy.get("psfDeterminerName")
        psfDeterminerPolicy = self.policy.getPolicy("psfDeterminerPolicy")
        self.psfDeterminer = measAlg.makePsfDeterminer(psfDeterminerName, psfDeterminerPolicy)

        
    def process(self, clipboard):
        self.log.log(Log.INFO, "Estimating PSF is in process")

        #grab exposure from clipboard
        exposure = clipboard.get(self.policy.get("inputKeys.exposure"))       
        sourceSet = clipboard.get(self.policy.get("inputKeys.sourceSet"))

        psfCandidateList = self.starSelector.selectStars(exposure, sourceSet)
        sdqaRatings = sdqa.SdqaRatingSet()
        psf, psfCellSet = self.psfDeterminer.determinePsf(exposure, psfCandidateList, sdqaRatings)
        
        clipboard.put(self.policy.get("outputKeys.psf"), psf)
        clipboard.put(self.policy.get("outputKeys.cellSet"), psfCellSet)
#        clipboard.put(self.policy.get("outputKeys.sourceSet"), psfSourceSet)
        clipboard.put(self.policy.get("outputKeys.sdqa"), sdqaRatings)

class PsfDeterminationStage(harnessStage.Stage):
    parallelClass = PsfDeterminationStageParallel

