#!/usr/bin/env python
import os, sys
import eups

import lsst.pex.harness.Stage as stage
import lsst.pex.policy as policy

from lsst.pex.logging import Log, Rec

import lsst.afw.detection as afwDet
import lsst.afw.image as afwImg
import lsst.afw.math as afwMath
import lsst.meas.algorithms as measAlg


class SourceMeasurementStage(Stage):
    """
    Description:
        This stage wraps the measurement of sources on an exposure.
        The exposure to detect should be in the clipboard along with the
        DetectionSet(s) to measure on that exposure. The keys for the
        exposure, its psf, and the DetectionSet(s) can be specified in the 
        policy file. If not specified, default keys will be used

    Policy Input: 
    - runMode (string): optional, default "process"    
    - exposureKey (string): optional, default "Exposure"
    - psfKey (string): optional, default "PSF"
    - positiveDetectionKey (string): optional, default "PositiveDetectionSet"
    - negativeDetectionKey (string): optional, default "NegativeDetectionSet"
    - outputKey (string): optional, default "SourceSet"

    Clipboard Input:
    - Exposure: REQUIRED with key specified in policy ("exposureKey")
    - Psf: optional with key specified in policy ("psfKey")
    - DetectionSet(s): with key(s) specified in policy 
            ("positiveDetectionKey", "negativeDetectionKey")

    ClipboardOutput:
    - Exposure from input with same key name
    - Psf from input with same key name
    - DetetectionSet(s) from input with same key name(s)
    - SourceSet with key name specified in policy ("outputKey")
    """

    def __init__(self, stageId = -1, policy = None):
        # call base constructor
        lsst.pex.harness.Stage.Stage.__init__(self,stageId, policy)
        # initialize a log
        self.log = Log(Log.getDefaultLog(), "SourceMeasurementStage")

    def preprocess(self):
        """
        Measure sources in the master process before any processing
        """
        if self._policy.exists('runMode') and \
                self._policy.getString('runMode') == 'preprocess':
            self.log.log(Log.INFO, "Measuring Sources in preprocess")
            self._measureSources()
        

    def process(self):
        """
        Measure sources in the worker process
        """
        if not self._policy.exists('runMode') or \
                self._policy.getString('runMode') == 'process':
            self.log.log(Log.INFO, "Measuring Sources in process")
            self._measureSources()

    
    def postprocess(self):
        """
        Measure sources in the master process after any processing
        """
        if self._policy.exists('runMode') and \
                self._policy.getString('runMode') == 'postprocess':
            self.log.log(Log.INFO, "Measuring Sources in postprocess")
            self._measureSources()
 

    def _measureSources(self):
        self._validatePolicy()
        queueLength = self.inputQueue.size();
        for i in xrange(queueLength):
            clipboard = self.inputQueue.getNextDataset()
            exposure = clipboard.get(self._exposureKey)
            psf = clipboard.get(self._psfKey)
            dsPositive = clipboard.get(self._positiveDetectionKey)
            dsNegative = clipboard.get(self._negativeDetectionKey)
            
            if exposure == None:
                # Don't have an exposure to work with
                # log warning and move on to next clipboard
                continue
                                    
            if psf == None:
                self.log.log(Log.DEBUG, "No PSF found on clipboard use default")                                        
                measureSources = measAlg.makeMeasureSources( \
                        exposure, \
                        self._measurePolicy)
            else:                
                measureSources = measAlg.makeMeasureSources( \
                        exposure, \
                        self._measurePolicy, \
                        psf)
            
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
                    footprintLIst.append(fp)
            
            sourceSet = afwDetection.SourceSet()
            sourceId = 0;
            for footprint in footprintList:
                source = afwDet.Source()
                try:
                    measureSources.apply(source, footprint)
                except:
                    self.log.log(Log.WARN, "Source measurement failed-skipping")
                    continue

                source.setId(sourceId)
                sourceList.append(source)
                sourceId+=1

            clipboard.put(outputKey, sourceSet)
            self.outputQueue.addDataset(clipboard)

                    
    def _validatePolicy(self): 
        if not self._policy.exists("measureObjects"):
            self.log.log(Log.WARN, "Using default measureObjects Policy")
            path = os.path.join("pipeline", "MeasureSources.paf")
            policyFile = policy.DefaultPolicyFile("meas_algorithm", path)
            self._measurePolicy = policy.Policy(policyFile)
        else:
            self._measurePolicy = self._policy

        if self._policy.exists("exposureKey"):
            self._exposureKey = self._policy.getString("exposureKey")
        else:
            self.log.log(Log.WARN, "Using default exposureKey=\"Exposure\"")
            self._exposureKey = "Exposure"

        if self._policy.exists("psfKey"):
            self._psfKey = self._policy.getString("psfKey")
        else:
            self.log.log(Log.WARN, "Using default psfKey=\"PSF\"")
            self._psfKey = "PSF"
           
        if self._policy.exists("positiveDetectionKey"):
            self._positiveDetectionKey = \
                    self._policy.getString("positiveDetectionKey")
        else:
            self.log.log(Log.WARN, "Using default positiveDetectionKey=\"PositiveDetectionSet\"")
            self._positiveDetectionKey = "PositiveDetectionSet"

        if self._policy.exists("negativeDetectionKey"):
            self._negativeDetectionKey = \
                    self._policy.getString("negativeDetectionkey")
        else:
            self.log.log(Log.WARN, "Using default negativeDetectionKey=\"NegativeDetectionSet\"")
            self._negativeDetectionKey = "NegativeDetectionSet"

        if self._policy.exists("outputKey"):
            self._outputKey = self._policy.getString("outputKey")
        else:
            self.log.log(Log.WARN, "Using default outputKey=\"SourceSet\"")
            self._outputKey = "SourceSet"
