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
    - optional PSF

    Clipboard output:
    - DetectionSet(s)- this stage produces up to 2 DetectionSet outputs
    """

    def __init__(self, stageId=-1, policy=None):
        SourceDetectionStage.__init__(self, stageId, policy)
        del self.log
        self.log = Log(Log.getDefaultLog(),
                        "lsst.meas.pipeline.AddAndDetectStage")
    def process(self):
        """
        Detect sources in the worker process
        """
        self.log.log(Log.INFO, "Detecting Sources in process")

        self._validatePolicy()
        clipboard = self.inputQueue.getNextDataset()

        exposureList = []
        for key in self._exposureKey:
            if not clipboard.contains(key):
                self.log.log(Log.FATAL, "Input missing - ignoring dataset")
                return

            exposureList.append(clipboard.get(key))
        
        addedExposure = self._addExposures(exposureList)

        psf = self._getOrMakePsf(clipboard)
        dsPositive, dsNegative = self._detectSourcesImpl(addedExposure, psf)
        #
        # Copy addedExposure's mask bits to the individual exposures
        #
        for e in exposureList:
            msk = e.getMaskedImage().getMask()
            msk |= addedExposure.getMaskedImage().getMask()
            del msk

        self._output(clipboard, dsPositive, dsNegative, None, psf) 

    def _addExposures(self, exposureList):
        exposure0 = exposureList[0]
        image0 = exposure0.getMaskedImage()

        addedImage = image0.Factory(image0, True)
        addedImage.setXY0(image0.getXY0())

        for exposure in exposureList[1:]:
            image = exposure.getMaskedImage()
            addedImage += image

        addedExposure = exposure0.Factory(addedImage, exposure0.getWcs())
        return addedExposure

    def _validatePolicy(self):
        SourceDetectionStage._validatePolicy(self)
        
        self._exposureKey = self._policy.getStringArray("exposureKey")

        
        

