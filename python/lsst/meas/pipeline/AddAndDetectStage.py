#!/usr/bin/env python

import lsst.pex.policy as policy
import lsst.pex.exceptions as pexExcept
from lsst.pex.logging import Log, Rec
import lsst.afw.image as afwImg
import lsst.afw.math as afwMath
import lsst.meas.algorithms as measAlg
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
    def process(self):
        """
        Detect sources in the worker process
        """
        log = pexLog.Log(pexLog.Log.getDefaultLog(),
                "lsst.meas.piepeline.AddAndDetectStage")

        log.log(log.INFO, "Detecting Sources in process")
        clipboard = self.inputQueue.getNextDataset()

        try:
            psf = self._getOrMakePsf(clipboard)
        except RuntimeError:
            log.log(Log.FATAL, "Source detection failed: missing a PSF")
            return
        # 
        # retrieve all listed exposures from the clipboard
        #
        exposureList = []
        exposureKeyList = self._policy.getArray("exposureKey")
        for key in exposureKeyList:
            if not clipboard.contains(key):
                log.log(log.FATAL, "Input missing - ignoring dataset")
                return
            exposureList.append(clipboard.get(key))
        #
        # Add all exposures
        #
        addedExposure = self._addExposures(exposureList)
        
        if display:
            maskedImage = addedExposure.getMaskedImage()
            ds9.mtv(maskedImage)
            del maskedImage

        if not ds9.getMaskPlaneColor("DETECTED_NEGATIVE"):
            ds9.setMaskPlaneColor("DETECTED_NEGATIVE", ds9.CYAN)

    
        #
        # perform detection
        #
        dsPositive, dsNegative = measAlg.detectSources(
                addedExposure, 
                psf,
                self._policy.get("detectionPolicy"))
        #
        # Copy addedExposure's mask bits to the individual exposures
        #
        for e in exposureList:
            msk = e.getMaskedImage().getMask()
            msk |= addedExposure.getMaskedImage().getMask()
            del msk

        
        #
        # output the detection sets, and psf to the clipboard
        # 
        self._output(dsPositive, dsNegative, psf)

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
