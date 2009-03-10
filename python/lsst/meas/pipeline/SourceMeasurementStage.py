#!/usr/bin/env python
import os, sys
import eups

import lsst.pex.harness.Stage as stage
import lsst.pex.policy as policy

from lsst.pex.logging import Log, Rec
import lsst.pex.exception as pexExcept
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
    - runMode (string): optional, default "process"  
    - exposure (policy array): optional, default:
		-inputKeys (strin
    - psfKey (string): optional, default "PSF"
    - positiveDetectionKey (string): optional, default "PositiveDetectionSet"
    - negativeDetectionKey (string): optional, default "NegativeDetectionSet"
    - outputKey (string): optional, default "SourceSet"

    Clipboard Input:
    - Exposure(s): REQUIRED with key specified in policy ("exposureKey")
    - Psf: optional with key specified in policy ("psfKey")
    - DetectionSet(s): with key(s) specified in policy 
            ("positiveDetectionKey", "negativeDetectionKey")

    Clipboard Output:
    - Exposure(s) from input with same key name
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
        clipboard = self.inputQueue.getNextDataset()		
		self._getClipboardData(clipboard)
						
	    #
        # Need to do something smart about merging positive and negative
        # detection sets.             
        #
		# For now, assume they are disjoint sets, so merge is trivial
		#   
		footprintList = afwDet.FootprintContainerT()
		if self._dsPositive != None:
			self.log.log(Log.DEBUG, "Positive DetectionSet found")
			for fp in self._dsPositive.getFootprints():
				footprintList.append(fp)
		if self._dsNegative != None:
			self.log.log(Log.DEBUG, "Negative DetectionSet found")
			for fp in self._dsNegative.getFootprints():
				footprintList.append(fp)
					
		for exposure, outKey in zip(self._exposureList, self._outputKeys):
            measureSources = measAlg.makeMeasureSources( exposure,
														 self._measurePolicy)			
			#rhl assures me psf will be in  exposure
			#psf = exposure.getPsf()     			
            #measureSources = measAlg.makeMeasureSources( exposure,
			#								    self._measurePolicy
			#  								    psf)			
            
            sourceSet = afwDetection.SourceSet()
            sourceId = 0;
            for footprint in footprintList:
                source = afwDet.Source()
				sourceList.append(source)
				source.setId(sourceId)
                sourceId += 1
                try:
                    measureSources.apply(source, footprint)
                except:
					# don't worry about measurement exceptions
           			pass                    
					            
			clipboard.put(outKey, sourceSet)
			
        self.outputQueue.addDataset(clipboard)
	
	
	def _getClipboardData(self, clipboard):
		self._exposureList = []
		for key in self._inputKeys:
			exposure = clipboard.get(key)
			if exposure == None:			
				raise pexExcept.NotFoundException("exposureKey %s"%key)
			self._exposureList.append(clipboard.get(key))
				            
        self._dsPositive = clipboard.get(self._positiveDetectionKey)
        self._dsNegative = clipboard.get(self._negativeDetectionKey)
		
    def _validatePolicy(self): 
        if not self._policy.exists("measureObjects"):
            self.log.log(Log.WARN, "Using default measureObjects Policy")
            path = os.path.join("pipeline", "MeasureSources.paf")
            policyFile = policy.DefaultPolicyFile("meas_algorithm", path)
            self._measurePolicy = policy.Policy(policyFile)
        else:
            self._measurePolicy = self._policy

        if self._policy.exists("data"):
            self._inputKeys = self._policy.getStringArray("data.inputKey")
			self._outputKeys = self._policy.getStringArray("data.outputKey")
        else:            
            self._inputKeys = ["Exposure"]
			self._outputKeys = ["SourceSet"]

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
