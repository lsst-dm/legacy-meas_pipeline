#!/usr/bin/env python
import os, sys

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
    - data: (policy array) list of (input, psf, output) keys 
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
            data, fpPositive, fpNegative = self._getClipboardData(clipboard)
        except pexExcept.LsstException, e:
            self.log.log(Log.FATAL, e.what())
         
        moPolicy = self._policy.get("measureObjects")
        # loop over all exposures
        for (measureFunctor, outKey) in data:
            sourceSet = measAlg.makeSourceSet(
                    measureFunctor,
                    fpPositive, fpNegative)

            #place SourceSet on the clipboard 
            clipboard.put(outKey, sourceSet)
            
            persistableSourceSet = afwDet.PersistableSourceVector(sourceSet)
            clipboard.put("persistable_" + outKey, persistableSourceSet) 
        
        #pass clipboard to outputQueue
        self.outputQueue.addDataset(clipboard)
    
	
    def _getClipboardData(self, clipboard):
        """
        private helper method for grabbing the clipboard data in a useful way
        """
        moPolicy = self._policy.get("measureObjects")
        
        data = []
        dataPolicyArray = self._policy.getArray("data")
        for dataPolicy in dataPolicyArray:             
            exposureKey = dataPolicy.get("exposureKey")
            psfKey = dataPolicy.get("psfKey")            
            outputKey = dataPolicy.get("outputKey")
            exposure = clipboard.get(exposureKey)
            if exposure == None:			
                raise Exception("Missing from clipboard: exposureKey %"\
                        %exposureKey)
            psf = clipboard.get(psfKey)
            if psf == None:			
                raise Exception("Missing from clipboard: psfKey %"\
                        %psfKey)

            measureSources = measAlg.makeMeasureSources(exposure, moPolicy, psf)
            data.append((measureSources, outputKey))

        positiveDetectionKey = self._policy.get("positiveDetectionKey")
        negativeDetectionKey = self._policy.get("negativeDetectionKey")
        dsPositive = clipboard.get(positiveDetectionKey)
        dsNegative = clipboard.get(negativeDetectionKey)
                
        if dsPositive == None and dsNegative ==None:
            raise Exception("Missing input DetectionSet")
        
        return (data, dsPositive.getFootprints(), dsNegative.getFootprints())

		
    def _validatePolicy(self): 
        #private helper method for validating my policy
        # for DC3a expects perfect policies!

        self._measurePolicy = self._policy.get("measureObjects")

        self._inputKeys = []
        self._outputKeys = []
        dataPolicyArray = self._policy.getArray("data")
        for dataPolicy in dataPolicyArray: 
            exposureKey = dataPolicy.get("exposureKey")
            psfKey = dataPolicy.get("psfKey")
            self._inputKeys.append((exposureKey, psfKey))
            self._outputKeys.append(dataPolicy.get("outputKey"))


