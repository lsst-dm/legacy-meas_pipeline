#!/usr/bin/env python

import lsst.pex.policy as pexPolicy
import lsst.pex.exceptions as pexExcept
from lsst.pex.logging import Log, Rec
import lsst.afw.image as afwImg
import lsst.afw.math as afwMath
import lsst.pex.harness.stage as harnessStage

import sourceDetection

class AddAndDetectStageParallel(harnessStage.ParallelProcessing):
    """
    Stage wrapper for adding together a list of images before performing
    detection.

    Policy Input:
    - exposureKey: List of background subtracted exposures to detect on
    - detectionPolicy: (optional)
    - psfPolicy: (optional)
    - positiveDetectionKey: (optional) output key for positive FootprintSet
    - negativeDetectionKey: (optional) output key for negative FootprintSet

    Clipboard Input:
    - Expsure for every exposureKey in policy
    - optional PSF

    Clipboard output:
    - FootprintSet(s)- this stage produces up to 2 DetectionSet outputs
    - psf used in detection
    - added Exposure
    """
    def setup(self):
        file = pexPolicy.DefaultPolicyFile("meas_pipeline", 
            "AddAndDetectStageDictionary.paf", "pipeline")
        defPolicy = pexPolicy.Policy.createPolicy(
            file, file.getRepositoryPath())

        if self.policy is None:
            self.policy = defPolicy
        else:
            self.policy.mergeDefaults(defPolicy)

    def process(self, clipboard):
        """
        Detect sources in the worker process
        """
        #grab list of background subtracted exposures
        exposureKeyList = self.policy.getStringArray("exposureKey") 
        exposureList = []
        for key in exposureKeyList:
            if not clipboard.contains(key):
                self.log.log(Log.FATAL, "Input missing - ignoring dataset")
                return
            exposureList.append(clipboard.get(key))
                
        addedExposure = sourceDetection.addExposures(exposureList)

        #get a smoothing psf
        psf = None
        if self.policy.exists("inputPsfKey"):
            psf = clipboard.get(self.policy.get("inputPsfKey"))
        elif self.policy.exists("psfPolicy"):
            psf = sourceDetection.makePsf(self.policy.get("psfPolicy"))

        #perform the detection
        dsPositive, dsNegative = sourceDetection.detectSources(
            addedExposure, psf, self.policy.get("detectionPolicy"))        
        
        #
        # Copy addedExposure's mask bits to the individual exposures
        #
        detectionMask = addedExposure.getMaskedImage().getMask()
        for e in exposureList:
            msk = e.getMaskedImage().getMask()
            msk |= detectionMask
            del msk

        del detectionMask

        #output products
        if not dsPositive is None:
            clipboard.put(self.policy.get("positiveDetectionKey"), dsPositive)
        if not dsNegative is None:
            clipboard.put(self.policy.get("negativeDetectionKey"), dsNegative)
        if not psf is None:
            clipboard.put(self.policy.get("psfKey"), psf)
        clipboard.put(self.policy.get("addedExposureKey"), addedExposure)

class AddAndDetectStage(harnessStage.Stage):
    """
    Defined for testing convenience
    """
    parallelClass = AddAndDetectStageParallel

