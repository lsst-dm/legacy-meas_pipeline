#!/usr/bin/env python
"""
Run with:
   python PsfDeterminationTest.py
"""

import sys, os, math
from math import *

import pdb
import unittest
import random
import time

import eups
import lsst.utils.tests as utilsTests
import lsst.pex.harness.Queue as pexQueue
import lsst.pex.harness.Clipboard as pexClipboard
import lsst.pex.policy as policy
import lsst.meas.pipeline as measPipe
import lsst.afw.detection as afwDet
import lsst.afw.image as afwImage

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class PsfDeterminationStageTestCase(unittest.TestCase):
    """A test case for SourceMeasurementStage.py"""

    def setUp(self):
        thresholdPolicy = policy.Policy()
        thresholdPolicy.add("minPixels", 1)
        thresholdPolicy.add("thresholdValue", 3.0)
        thresholdPolicy.add("thresholdType", "value")
        thresholdPolicy.add("thresholdPolarity", "positive")

        psfPolicy = policy.Policy()
        psfPolicy.add("algorithm", "DoubleGaussian")
        psfPolicy.add("width", 15)
        psfPolicy.add("height", 15)
        psfPolicy.add("parameter", 5.0/(2*sqrt(2*log(2))))

        detectPolicy1 = policy.Policy()
        detectPolicy1.add("exposureKey", "calibratedExposure0")
        detectPolicy1.add("psfPolicy", psfPolicy)
        detectPolicy1.add("detectionPolicy", thresholdPolicy)
        detectPolicy1.add("smoothingPsfKey", "smoothingPsf")
        detectPolicy1.add("positiveDetectionKey", "positiveFootprintSet")
        
        dataPolicy1 = policy.Policy()
        dataPolicy1.add("exposureKey", "calibratedExposure0")
        dataPolicy1.add("outputKey", "sourceSet0")
        
        moPolicy = policy.Policy()
        moPolicy.add("centroidAlgorithm", "SDSS")
        moPolicy.add("shapeAlgorithm", "SDSS")
        moPolicy.add("photometryAlgorithm", "NAIVE")
        moPolicy.add("apRadius", 3.0)

        measPolicy1 = policy.Policy()
        measPolicy1.add("data", dataPolicy1)
        measPolicy1.add("measureObjects", moPolicy)
        measPolicy1.add("positiveDetectionKey", "positiveFootprintSet")
        measPolicy1.add("negativeDetectionKey", "negativeFootprintSet")
        measPolicy1.add("psfKey", "smoothingPsf")
       
        dataPolicy2 = policy.Policy()
        dataPolicy2.add("exposureKey", "calibratedExposure0")
        dataPolicy2.add("sourceSetKey", "sourceSet0")
        dataPolicy2.add("outputPsfKey", "psfModel")
        dataPolicy2.add("outputCellSetKey", "psfCellSet")

        psfDeterminationPolicy = policy.Policy()
        psfDeterminationPolicy.add("data", dataPolicy2)
        psfDeterminationPolicy.add("fluxLim", 1000)
        psfDeterminationPolicy.add("sizeCellX", 512)
        psfDeterminationPolicy.add("sizeCellY", 512)
        psfDeterminationPolicy.add("nStarPerCell", 3)
        psfDeterminationPolicy.add("kernelSize", 21)
        psfDeterminationPolicy.add("nEigenComponents", 2)
        psfDeterminationPolicy.add("spatialOrder", 2)
        psfDeterminationPolicy.add("nStarPerCellSpatialFit", 5)
        psfDeterminationPolicy.add("tolerance", 0.1)
        psfDeterminationPolicy.add("reducedChi2ForPsfCandidates", 2.0)
        psfDeterminationPolicy.add("nIterForPsf", 3)

        
        clipboard1 = pexClipboard.Clipboard() 
       
        filename = os.path.join(eups.productDir("afwdata"),
                                "CFHT", "D4", 
                                "cal-53535-i-797722_1")
        loadExp = afwImage.ExposureF(filename)
        bbox = afwImage.BBox(afwImage.PointI(32, 2), 512, 512)
        testExp = afwImage.ExposureF(loadExp, bbox)
        clipboard1.put("calibratedExposure0", testExp)

        inQueue1 = pexQueue.Queue() 
        inQueue1.addDataset(clipboard1)
        detToMeasQueue = pexQueue.Queue()
        measToPsfQueue = pexQueue.Queue()
        self.outQueue1 = pexQueue.Queue()
        
        self.detectStage1 = measPipe.SourceDetectionStage(0, detectPolicy1)
        self.detectStage1.initialize(detToMeasQueue, inQueue1)
        self.detectStage1.setUniverseSize(1)
        self.detectStage1.setRun("psfDeterminationTest")
        self.measStage1 = measPipe.SourceMeasurementStage(1, measPolicy1)
        self.measStage1.initialize(measToPsfQueue, detToMeasQueue)
        self.measStage1.setUniverseSize(1)
        self.measStage1.setRun("psfDeterminationTest")
        self.psfDeterminationStage = \
                measPipe.PsfDeterminationStage(2, psfDeterminationPolicy)
        self.psfDeterminationStage.initialize(self.outQueue1, measToPsfQueue)
        self.psfDeterminationStage.setUniverseSize(1)
        self.psfDeterminationStage.setRun("psfDeterminationTest")

    def tearDown(self):
        del self.measStage1
        del self.detectStage1
        del self.psfDeterminationStage
        del self.outQueue1

    def sanityCheckTest(self):
        self.detectStage1.process()
        self.measStage1.process()
        self.psfDeterminationStage.process()
        clipboard = self.outQueue1.getNextDataset()
        assert(clipboard.contains("sourceSet0"))
        assert(clipboard.contains("psfModel"))
        assert(clipboard.contains("psfCellSet"))

def suite():
    """Returns a suite containing all the test cases in this module."""

    utilsTests.init()

    suites = []
    suites += unittest.makeSuite(PsfDeterminationStageTestCase)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(exit=False):
    """Run the tests"""
    utilsTests.run(suite(), exit)

if __name__ == "__main__":
    run(True)

