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
        lsst.pex.harness.Stage.Stage.__init__(stageId, policy)
        # initialize a log
        self.log = Log(Log.getDefaultLog(), "SourceMeasurementStage")
    
    def preprocess(self):
        """
        Measure sources in the master process before any processing
        """
        if self._policy.exists('runMode') and \
                self._policy.getString('runMode') == 'preprocess':
            self.log.log(Log.INFO, "Measuring Sources in preprocess")
            self.measureSources()
        

    def process(self):
        """
        Measure sources in the worker process
        """
        if not self._policy.exists('runMode') or \
                self._policy.getString('runMode') == 'process':
            self.log.log(Log.INFO, "Measuring Sources in process")
            self.measureSources()
    
     
    def postprocess(self):
        """
        Measure sources in the master process after any processing
        """
        if self._policy.exists('runMode') and \
                self._policy.getString('runMode') == 'postprocess':
            self.log.log(Log.INFO, "Measuring Sources in postprocess")
            self.measureSources()
    
    
    def measureSources(self):
        self.__validatePolicy__()  	
        clipboard = self.inputQueue.getNextDataset()		
        
        #this may raise exceptions
        try:
            input = self.__getClipboardData__(clipboard)
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
		
        # loop over all exposure, psf pairs
        for exposure, outKey in zip(exposureList, self.__outputKeys__):
            exposure = data 
            
            measureSources = measAlg.makeMeasureSources(exposure,
           								                self.__measurePolicy__,
           								                psf)
            
            sourceSet = afwDetection.SourceSet()
            sourceId = 0;
            for footprint in footprintList:
                source = afwDet.Source()
                sourceList.append(source)
                source.setId(sourceId)
                sourceId += 1
                try:
                    measureSources.apply(source, footprint)
                except Exception, e:
                    # don't worry about measurement exceptions
                    # although maybe I should log them
                    self.log.log(Log.WARN, e.what())
            
            #place SourceSet on the clipboard 
	        clipboard.put(outKey, sourceSet)
        
        #pass clipboard to outputQueue	    	
        self.outputQueue.addDataset(clipboard)
    
	
    def __getClipboardData__(self, clipboard):
        #private helped method for grabbing the clipboard data in a useful way 
        
        psf = clipboard.get(self.__psfKey__)
        exposureList = []
        for exposureKey in self.__inputKeys__: 
            exposure = clipboard.get(exposureKey)
            if exposure == None:			
                raise pexExcept.NotFoundException("exposureKey %"%exposureKey)
            exposureList.append(exposure)
        
        dsPositive = clipboard.get(self._positiveDetectionKey)
        dsNegative = clipboard.get(self._negativeDetectionKey)
        
        return (exposureList, psf, dsPositive, dsNegative)
		
    def __validatePolicy__(self): 
        #private helper method for validating my policy
        #generates default policy values as needed
        
        self.__measurePolicy__ = self._policy.getPolicy("measureObjects")
        self.__psfKey__ = self._policy.getString("psfKey")
        self.__inputKeys__ = []
        self.__outputKeys__ = []
        dataPolicyArray = self._policy.getPolicyArray("data")
        for dataPolicy in dataPolicyArray: 
            self.__inputKeys__.append(dataPolicy.getString("exposureKey"))
            self.__outputKeys__.append(dataPolicy.getString("outputKey"))

        self.__positiveDetectionKey__ = self._policy.get("positiveDetectionKey")
        self.__negativeDetectionKey__ = self._policy.get("negativeDetectionkey")
