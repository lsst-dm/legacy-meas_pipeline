#!/usr/bin/env python
import os, sys
import eups

from lsst.pex.harness.Stage import Stage

import lsst.pex.policy as policy

from lsst.pex.logging import Log, Rec
import lsst.pex.exceptions as pexExcept
import lsst.afw.detection as afwDet
import lsst.afw.image as afwImg
import lsst.afw.math as afwMath
import lsst.meas.algorithms as measAlg


class SourceMeasurementStage(Stage):
    """
    Description:
        This stage wraps the measurement of sources on an exposure.
        The exposures to measure on should be in the clipboard along with the
        DetectionSet(s) to measure on those exposures. The keys for the
        exposures, and the DetectionSet(s) can be specified in the 
        policy file. If not specified, default keys will be used

    Policy Input: 
    - data: (policy array) list of (input, output) keys 
    - psfKey (string): optional, default "psf"
    - positiveDetectionKey (string): optional, default "positiveDetectionSet"
    - negativeDetectionKey (string): optional, default "negativeDetectionSet"

    Clipboard Input:
    - Exposure(s): key specified policy by attribute data.exposureKey
    - Psf: with key specified by policy attribute psfKey
    - DetectionSet(s): with key(s) specified in policy 
            ("positiveDetectionKey", "negativeDetectionKey")

    Clipboard Output:
    - Exposure(s) from input with same key name
    - Psf from input with same key name
    - DetetectionSet(s) from input with same key name(s)
    - SourceSet with key specified by policy data.outputKey
    """

    def __init__(self, stageId = -1, policy = None):
        # call base constructor
        Stage.__init__(self, stageId, policy)
        # initialize a log
        self.log = Log(Log.getDefaultLog(), "SourceMeasurementStage")
    
    def process(self):
        """
        Measure sources in the worker process
        """
        self.log.log(Log.INFO, "Measuring Sources in process")
        
        self._validatePolicy()
        clipboard = self.inputQueue.getNextDataset()		
        
        #this may raise exceptions
        try:
            input = self._getClipboardData(clipboard)
            exposureList, psf, dsPositive, dsNegative = input
        except pexExcept.LsstException, e:
            self.log.log(Log.FATAL, e.what())
         
        #
        # Need to do something smart about merging positive and negative
        # detection sets.             
        #
        # For now, assume they are disjoint sets, so merge is trivial
        #   
        footprintList = afwDet.FootprintContainerT()
        if dsPositive != None:
            self.log.log(Log.DEBUG, "Positive DetectionSet found")
            for fp in dsPositive.getFootprints():
                footprintList.append(fp)
        if dsNegative != None:
            self.log.log(Log.DEBUG, "Negative DetectionSet found")
            for fp in dsNegative.getFootprints():
                footprintList.append(fp)
        
        # loop over all exposures
        for exposure, outKey in zip(exposureList, self._outputKeys):
            measureSources = measAlg.makeMeasureSources(exposure,
                                                        self._measurePolicy, 
                                                        psf)
            sourceSet = afwDet.SourceSet()
            sourceId = 0;
            for footprint in footprintList:
                source = afwDet.Source()
                sourceSet.append(source)
                source.setId(sourceId)
                sourceId += 1
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
        
        #pass clipboard to outputQueue
        self.outputQueue.addDataset(clipboard)
    
	
    def _getClipboardData(self, clipboard):
        #private helped method for grabbing the clipboard data in a useful way 
        
        psf = clipboard.get(self._psfKey)
        exposureList = []
        for exposureKey in self._inputKeys: 
            exposure = clipboard.get(exposureKey)
            if exposure == None:			
                raise Exception("Missing from clipboard: exposureKey %"%exposureKey)
            exposureList.append(exposure)
        
        dsPositive = clipboard.get(self._positiveDetectionKey)
        dsNegative = clipboard.get(self._negativeDetectionKey)
        if dsPositive == None and dsNegative ==None:
            raise Exception("Missing DetectionSet")
        return (exposureList, psf, dsPositive, dsNegative)
		
    def _validatePolicy(self): 
        #private helper method for validating my policy
        # for DC3a expects perfect policies!

        self._measurePolicy = self._policy.getPolicy("measureObjects")
        self._psfKey = self._policy.getString("psfKey")
        self._inputKeys = []
        self._outputKeys = []
        dataPolicyArray = self._policy.getPolicyArray("data")
        for dataPolicy in dataPolicyArray: 
            self._inputKeys.append(dataPolicy.getString("exposureKey"))
            self._outputKeys.append(dataPolicy.getString("outputKey"))

        self._positiveDetectionKey = self._policy.get("positiveDetectionKey")
        self._negativeDetectionKey = self._policy.get("negativeDetectionkey")
