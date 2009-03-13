#!/usr/bin/env python

import lsst.pex.policy as policy
import lsst.pex.exceptions as pexExcept
from lsst.pex.logging import Log, Rec
import lsst.afw.image as afwImg
import lsst.afw.math as afwMath
from SourceDetectionStage import SourceDetectionStage

class AddAndDetectStage(SourceDetectionStage):
    """
    Stage wrapper for adding together a list of images before performing
    detection.

    Policy Input:
    - exposureKey: List of exposures to stack for detection
    - detectionPolicy: (optional)
    - psfPolicy: (optional)
    - positiveDetectionKey: (optional) output key for positive DetectionSet
    - negativeDetectionKey: (optional) output key for negative DetectionSet

    Clipboard Input:
    - Expsure for every exposureKey in policy

    Clipboard output:
    - DetectionSet(s)- this stage produces up to 2 DetectionSet outputs
    """

    def preprocess(self):
        """
        Detect sources in the master process before any processing
        """
        if self._policy.exists('runMode') and \
                self._policy.getString('runMode') == 'preprocess':
            self.log.log(Log.INFO, "Detecting Sources in preprocess")
            self.addAndDetect()
        

    def process(self):
        """
        Detect sources in the worker process
        """
        if not self._policy.exists('runMode') or \
                self._policy.getString('runMode') == 'process':
            self.log.log(Log.INFO, "Detecting Sources in process")
            self.addAndDetect()

    
    def postprocess(self):
        """
        Detect sources in the master process after any processing
        """
        if self._policy.exists('runMode') and \
                self._policy.getString('runMode') == 'postprocess':
            self.log.log(Log.INFO, "Detecting Sources in postprocess")
            self.addAndDetect()


    def addAndDetect(self):
        self._validatePolicy()
        clipboard = self.inputQueue.getNextDataset()

        exposureList = []
        for key in self._exposureKey:
            if not clipboard.contains(key):
                self.log.log(Log.FATAL, "Input missing - ignoring dataset")
                return

            exposureList.append(clipboard.get(key))
        
        addedExposure = _addExposures(exposureList)
        dsPositive, dsNegative = self._detectSourcesImpl(addedExposure)
        self._output(clipboard, dsPositive, dsNegative) 

    def _addExposures(exposureList):
        exposure0 = exposureList[0]
        image0 = exposure0.getMaskedImage()

        addedImage = image0.Factory(image0.getDimensions(), 
                                    image0.getMask().getMaskPlaneDict())
        addedImage.setXYO(image0.getXY0())
        addedImage <<= image0

        for exposure in exposureList[1:]:
            image = exposure.getMaskedImage()
            addedImage += image

        addedExposure = exposure0.Factory(addedImage, exposure0.getWcs())
        return addedExposure

    def _validatePolicy(self):
        SourceDetectionStage._validatePolicy(self)
        
        self._exposureKey = self._policy.getStringArray("exposureKey")

        
        

