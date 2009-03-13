#!/usr/bin/env python
"""
Run with:
   python MeasureTest.py
"""

import sys, os, math
from math import *

import pdb
import unittest
import random
import time

import lsst.utils.tests as utilsTests
import lsst.pex.harness.Queue as pexQueue
import lsst.pex.harness.Clipboard as pexClipboard
import lsst.pex.policy as policy
import lsst.meas.pipeline as measPipe
import lsst.afw.detection as afwDet
import lsst.afw.image as afwImage

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class MeasureStageTestCase(unittest.TestCase):
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
        detectPolicy1.add("smoothingPsfKey", "psfModel")
        detectPolicy1.add("positiveDetectionKey", "positiveFootprintSet")
        
        detectPolicy2 = policy.Policy()
        detectPolicy2.add("exposureKey", "calibratedExposure0")
        detectPolicy2.add("exposureKey", "calibratedExposure1")
        detectPolicy2.add("psfPolicy", psfPolicy)
        detectPolicy2.add("detectionPolicy", thresholdPolicy)
        detectPolicy2.add("smoothingPsfKey", "psfModel")
        detectPolicy2.add("positiveDetectionKey", "positiveFootprintSet")
        
        dataPolicy1 = policy.Policy()
        dataPolicy1.add("exposureKey", "calibratedExposure0")
        dataPolicy1.add("outputKey", "sourceSet0")
        dataPolicy2 = policy.Policy()
        dataPolicy2.add("exposureKey", "calibratedExposure1")
        dataPolicy2.add("outputKey", "sourceSet1")
       
        moPolicy = policy.Policy()
        moPolicy.add("centroidAlgorithm", "SDSS")
        moPolicy.add("shapeAlgorithm", "SDSS")
        moPolicy.add("photometryAlgorithm", "NAIVE")
        moPolicy.add("apRadius", 3.0)

        measPolicy2 = policy.Policy()
        measPolicy2.add("data", dataPolicy1)
        measPolicy2.add("data", dataPolicy2)
        measPolicy2.add("measureObjects", moPolicy)
        measPolicy2.add("positiveDetectionKey", "positiveFootprintSet")
        measPolicy2.add("negativeDetectionKey", "negativeFootprintSet")
        measPolicy2.add("psfKey", "psfModel")

        measPolicy1 = policy.Policy()
        measPolicy1.add("data", dataPolicy1)
        measPolicy1.add("measureObjects", moPolicy)
        measPolicy1.add("positiveDetectionKey", "positiveFootprintSet")
        measPolicy1.add("negativeDetectionKey", "negativeFootprintSet")
        measPolicy1.add("psfKey", "psfModel")
       
        clipboard1 = pexClipboard.Clipboard() 
        clipboard2 = pexClipboard.Clipboard()
        img = afwImage.MaskedImageF(512, 512)
        img.set( 10, 1, 1)
        exp = afwImage.ExposureF(img)

        clipboard1.put("calibratedExposure0", exp)
        clipboard2.put("calibratedExposure0", exp) 
        clipboard2.put("calibratedExposure1", exp)

        inQueue1 = pexQueue.Queue() 
        inQueue2 = pexQueue.Queue()
        inQueue1.addDataset(clipboard1)
        inQueue2.addDataset(clipboard2)
        detToMeasQueue1 = pexQueue.Queue()
        detToMeasQueue2 = pexQueue.Queue()
        self.outQueue1 = pexQueue.Queue()
        self.outQueue2 = pexQueue.Queue()
        
        self.detectStage1 = measPipe.SourceDetectionStage(0, detectPolicy1)
        self.detectStage1.initialize(detToMeasQueue1, inQueue1)
        self.detectStage1.setUniverseSize(1)
        self.measStage1 = measPipe.SourceMeasurementStage(1, measPolicy1)
        self.measStage1.initialize(self.outQueue1, detToMeasQueue1)
        self.measStage1.setUniverseSize(1)

        self.detectStage2 = measPipe.AddAndDetectStage(0, detectPolicy2)
        self.detectStage2.initialize(detToMeasQueue2, inQueue2)
        self.detectStage2.setUniverseSize(1)
        self.measStage2 = measPipe.SourceMeasurementStage(1, measPolicy2)
        self.measStage2.initialize(self.outQueue2, detToMeasQueue2)
        self.measStage2.setUniverseSize(1)


    def tearDown(self):
        del self.measStage1
        del self.measStage2
        del self.detectStage1
        del self.detectStage2
        del self.outQueue1
        del self.outQueue2

    def testSingleInputExposure(self):
        self.detectStage1.process()
        self.measStage1.process()
        clipboard = self.outQueue1.getNextDataset()
        assert(clipboard.contains("sourceSet0"))
        assert(clipboard.contains("persistable_sourceSet0"))

    def testMultipleInputExposure(self):
        self.detectStage2.process()
        self.measStage2.process()
        clipboard = self.outQueue2.getNextDataset()
        assert(clipboard.contains("sourceSet0"))
        assert(clipboard.contains("sourceSet1"))
        assert(clipboard.contains("persistable_sourceSet0"))
        assert(clipboard.contains("persistable_sourceSet1"))

def suite():
    """Returns a suite containing all the test cases in this module."""

    utilsTests.init()

    suites = []
    suites += unittest.makeSuite(MeasureStageTestCase)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(exit=False):
    """Run the tests"""
    utilsTests.run(suite(), exit)

if __name__ == "__main__":
    run(True)

