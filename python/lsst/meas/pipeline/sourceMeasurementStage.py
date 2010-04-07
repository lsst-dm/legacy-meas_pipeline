#!/usr/bin/env python
import os, sys

import lsst.pex.harness.stage            as harnessStage

import lsst.pex.policy                   as pexPolicy

from   lsst.pex.logging                  import Log, Rec
import lsst.pex.exceptions               as pexExcept
import lsst.afw.detection                as afwDet
import lsst.afw.image                    as afwImg
import lsst.afw.math                     as afwMath
import lsst.meas.algorithms              as measAlg
import lsst.meas.utils.sourceMeasurement as srcMeas

class SourceMeasurementStageParallel(harnessStage.ParallelProcessing):
    """
    Description:
        This stage wraps the measurement of sources on an exposure.
        The exposures to measure on should be in the clipboard along with the
        FootprintSet(s) to measure on those exposures. The keys for the
        exposures, and the FootprintSet(s) can be specified in the 
        policy file. If not specified, default keys will be used
    """
    def setup(self):
        self.log = Log(self.log, "SourceMeasurementStage - parallel")

        policyFile = pexPolicy.DefaultPolicyFile("meas_pipeline", 
            "SourceMeasurementStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath(), True)

        if self.policy is None:
            self.policy = pexPolicy.policy()
        self.policy.mergeDefaults(defPolicy.getDictionary())
        
    def process(self, clipboard):
        """
        Measure sources in the worker process
        """
        self.log.log(Log.INFO, "Measuring Sources in process")
        
        #this may raise exceptions
        try:
            measurePolicy, exposure, psf, positiveDetection, negativeDetection = \
                           self.getClipboardData(clipboard)
        except pexExcept.LsstException, e:
            self.log.log(Log.FATAL, e.what())
         
        #
        # Need to do something smart about merging positive and negative
        # detection sets.             
        #
        # For now, assume they are disjoint sets, so merge is trivial
        #
        footprintLists = []
        if positiveDetection:
            self.log.log(Log.DEBUG, "Positive FootprintSet found")
            isNegative = False
            footprintLists.append([positiveDetection.getFootprints(), isNegative])

        if negativeDetection:
            self.log.log(Log.DEBUG, "Negative FootprintSet found")
            isNegative = True
            footprintLists.append([negativeDetection.getFootprints(), isNegative])

        sourceSet = srcMeas.sourceMeasurement(exposure, psf, footprintLists, measurePolicy)
        
        # place SourceSet on the clipboard
        sourceKey = self.policy.get("outputKeys.sources")
        clipboard.put(sourceKey, sourceSet)
        clipboard.put(sourceKey + "_persistable", afwDet.PersistableSourceVector(sourceSet))
        
    def getClipboardData(self, clipboard):
        #private helped method for grabbing the clipboard data in a useful way 

        measurePolicy = self.policy.getPolicy("measureObjects")

        exposure = clipboard.get(self.policy.get("inputKeys.exposure"))
        psf = clipboard.get(self.policy.get("inputKeys.psf"))
        positiveDetection = clipboard.get(self.policy.get("inputKeys.positiveDetection"))
        negativeDetection = clipboard.get(self.policy.get("inputKeys.negativeDetection"))

        if not positiveDetection and not negativeDetection:
            raise Exception("Missing input FootprintSets")

        return measurePolicy, exposure, psf, positiveDetection, negativeDetection
		
class SourceMeasurementStage(harnessStage.Stage):
    parallelClass = SourceMeasurementStageParallel
