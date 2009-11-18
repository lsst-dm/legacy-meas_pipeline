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

    Policy Input: 
    - data: (policy array) list of (input, psf, output) keys 
    - positiveDetectionKey (string): optional, default "positiveFootprintSet"
    - negativeDetectionKey (string): optional, default "negativeFootprintSet"

    Clipboard Input:
    - Exposure(s): key specified policy by attribute data.exposureKey
    - Psf: with key specified by policy attribute psfKey
    - FootprintSet(s): with key(s) specified in policy 
            ("positiveDetectionKey", "negativeDetectionKey")

    Clipboard Output:
    - Exposure(s) from input with same key name
    - Psf from input with same key name
    - DetetectionSet(s) from input with same key name(s)
    - SourceSet with key specified by policy data.outputKey
    """
    def setup(self):
        self.log = Log(self.log, "SourceMeasurementStage - parallel")

        policyFile = pexPolicy.DefaultPolicyFile("meas_pipeline", 
            "SourceMeasurementStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(
            policyFile, policyFile.getRepositoryPath())

        if self.policy is None:
            self.policy = pexPolicy.policy()
        self.policy.mergeDefaults(defPolicy)

        
    def process(self, clipboard):
        """
        Measure sources in the worker process
        """
        self.log.log(Log.INFO, "Measuring Sources in process")
        
        self.validatePolicy()
        
        #this may raise exceptions
        try:
            input = self.getClipboardData(clipboard)
            exposureAndPsfList, dsPositive, dsNegative = input
        except pexExcept.LsstException, e:
            self.log.log(Log.FATAL, e.what())
         
        #
        # Need to do something smart about merging positive and negative
        # detection sets.             
        #
        # For now, assume they are disjoint sets, so merge is trivial
        #
        footprintLists = []
        if dsPositive != None:
            self.log.log(Log.DEBUG, "Positive FootprintSet found")
            isNegative = False
            footprintLists.append([dsPositive.getFootprints(), isNegative])

        if dsNegative != None:
            self.log.log(Log.DEBUG, "Negative FootprintSet found")
            isNegative = True
            footprintLists.append([dsNegative.getFootprints(), isNegative])
        
        # loop over all exposures
        for (exposure,psf), outKey in zip(exposureAndPsfList, self._outputKeys):
            measureSources = measAlg.makeMeasureSources(exposure, self._measurePolicy, psf)

            sourceSet = afwDet.SourceSet()
            sourceId = 0;
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
            clipboard.put(outKey, sourceSet)
            
            persistableSourceSet = afwDet.PersistableSourceVector(sourceSet)
            clipboard.put("persistable_" + outKey, persistableSourceSet) 
        
	
    def getClipboardData(self, clipboard):
        #private helped method for grabbing the clipboard data in a useful way 

        exposureAndPsfList = []
        for exposureKey, psfKey in self._inputKeys: 
            exposure = clipboard.get(exposureKey)
            if exposure == None:			
                raise Exception("Missing from clipboard: exposureKey %"\
                        %exposureKey)
            psf = clipboard.get(psfKey)
            if exposure == None:			
                raise Exception("Missing from clipboard: psfKey %"\
                        %psfKey)
            
            exposureAndPsfList.append((exposure, psf))
        
        dsPositive = clipboard.get(self._positiveDetectionKey)
        dsNegative = clipboard.get(self._negativeDetectionKey)
        if dsPositive == None and dsNegative ==None:
            raise Exception("Missing input FootprintSet")
        return (exposureAndPsfList, dsPositive, dsNegative)
		
    def validatePolicy(self): 
        #private helper method for validating my policy
        # for DC3a expects perfect policies!

        self._measurePolicy = self.policy.getPolicy("measureObjects")
        self._inputKeys = []
        self._outputKeys = []
        dataPolicyArray = self.policy.getPolicyArray("data")
        for dataPolicy in dataPolicyArray: 
            exposureKey = dataPolicy.getString("exposureKey")
            psfKey = dataPolicy.getString("psfKey")
            self._inputKeys.append((exposureKey, psfKey))
            self._outputKeys.append(dataPolicy.getString("outputKey"))

        self._positiveDetectionKey = self.policy.get("positiveDetectionKey")
        self._negativeDetectionKey = self.policy.get("negativeDetectionKey")

class SourceMeasurementStage(harnessStage.Stage):
    parallelClass = SourceMeasurementStageParallel
