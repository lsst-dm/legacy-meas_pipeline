#!/usr/bin/env python
import os, sys

import lsst.pex.harness.stage as harnessStage

import lsst.pex.policy as pexPolicy

from lsst.pex.logging import Log, Rec
import lsst.pex.exceptions as pexExcept
import lsst.afw.detection as afwDet
import lsst.afw.image as afwImg
import lsst.afw.math as afwMath
import lsst.meas.algorithms as measAlg


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
            measurePolicy, exposureList, psfList, positiveDetection, negativeDetection = \
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
        
        # loop over all exposures
        sourceSet = afwDet.SourceSet()
        sourceId = 0;

        for exposure, psf in zip(exposureList, psfList):
            measureSources = measAlg.makeMeasureSources(exposure, measurePolicy, psf)
        
            for footprintList, isNegative in footprintLists:
                for footprint in footprintList:
                    source = afwDet.Source()
                    sourceSet.append(source)
                    source.setId(sourceId)
                    sourceId += 1

                    detectionBits = measAlg.Flags.BINNED1
                    if isNegative:
                        detectionBits |= measAlg.Flags.DETECT_NEGATIVE

                    source.setFlagForDetection(source.getFlagForDetection() | detectionBits);

                    try:
                        measureSources.apply(source, footprint)
                    except Exception, e:
                        # don't worry about measurement exceptions
                        # although maybe I should log them
                        self.log.log(Log.WARN, str(e))
            
            #place SourceSet on the clipboard
            clipboard.put(self.policy.get("outputKeys.sources"), sourceSet)
	
    def getClipboardData(self, clipboard):
        #private helped method for grabbing the clipboard data in a useful way 

        measurePolicy = self.policy.getPolicy("measureObjects")

        exposureList = []
        for e in self.policy.getArray("inputKeys.exposure"):
            exposureList.append(clipboard.get(e))

        psfList = []
        for p in self.policy.getArray("inputKeys.psf"):
            psfList.append(clipboard.get(p))

        positiveDetection = clipboard.get(self.policy.get("inputKeys.positiveDetection"))
        negativeDetection = clipboard.get(self.policy.get("inputKeys.negativeDetection"))

        if not positiveDetection and not negativeDetection:
            raise Exception("Missing input FootprintSets")

        return (measurePolicy, exposureList, psfList, positiveDetection, negativeDetection)
		
class SourceMeasurementStage(harnessStage.Stage):
    parallelClass = SourceMeasurementStageParallel
