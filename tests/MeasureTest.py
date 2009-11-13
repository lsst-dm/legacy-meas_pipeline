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

import eups
import lsst.utils.tests as utilsTests
import lsst.pex.harness.Clipboard as pexClipboard
import lsst.pex.policy as pexPolicy
import lsst.meas.pipeline as measPipe
import lsst.afw.detection as afwDet
import lsst.afw.image as afwImage
from lsst.pex.harness.simpleStageTester import SimpleStageTester

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class MeasureStageTestCase(unittest.TestCase):
    """A test case for SourceMeasurementStage.py"""

    def setUp(self):
        filename = os.path.join(eups.productDir("afwdata"),
                                "CFHT", "D4", 
                                "cal-53535-i-797722_1")
        bbox = afwImage.BBox(afwImage.PointI(32,32), 512, 512)
        self.exposure =  afwImage.ExposureF(filename, 0, bbox)
    def tearDown(self):
        del self.exposure

    def testSingleInputExposure(self):
        file = pexPolicy.DefaultPolicyFile("meas_pipeline", 
                "tests/MeasureTestDetect.paf")
        detectPolicy = pexPolicy.Policy.createPolicy(file)
        file = pexPolicy.DefaultPolicyFile("meas_pipeline", 
                "tests/MeasureTest1.paf")
        measurePolicy = pexPolicy.Policy.createPolicy(file)

        tester = SimpleStageTester()
        tester.addStage(measPipe.SourceDetectionStage(detectPolicy))
        tester.addStage(measPipe.SourceMeasurementStage(measurePolicy))

        clipboard = pexClipboard.Clipboard()
        clipboard.put("calibratedExposure0", self.exposure)
        
        outWorker = tester.runWorker(clipboard)

        assert(outWorker.contains("sourceSet0"))
        assert(outWorker.contains("persistable_sourceSet0"))

        del clipboard
        del outWorker

    def testMultipleInputExposure(self):
        file = pexPolicy.DefaultPolicyFile("meas_pipeline", 
                "tests/AddDetectTest2.paf")
        detectPolicy = pexPolicy.Policy.createPolicy(file)
        file = pexPolicy.DefaultPolicyFile("meas_pipeline", 
                "tests/MeasureTest1.paf")
        measurePolicy = pexPolicy.Policy.createPolicy(file)

        tester = SimpleStageTester()
        tester.addStage(measPipe.AddAndDetectStage(detectPolicy))
        tester.addStage(measPipe.SourceMeasurementStage(measurePolicy))

        clipboard = pexClipboard.Clipboard()
        clipboard.put("calibratedExposure0", self.exposure)
        clipboard.put("calibratedExposure1", self.exposure)
        
        outWorker = tester.runWorker(clipboard)

        assert(outWorker.contains("sourceSet0"))
        assert(outWorker.contains("persistable_sourceSet0"))
        assert(outWorker.contains("sourceSet0"))
        assert(outWorker.contains("sourceSet1"))    
        assert(outWorker.contains("persistable_sourceSet0"))
        assert(outWorker.contains("persistable_sourceSet1"))

        del outWorker
        del clipboard

def suite():
    """Returns a suite containing all the test cases in this module."""

    utilsTests.init()

    suites = []

    if not eups.productDir("afwdata"):
        print >> sys.stderr, "afwdata is not setting up; skipping test"
    else:
        suites += unittest.makeSuite(MeasureStageTestCase)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(exit=False):
    """Run the tests"""
    utilsTests.run(suite(), exit)

if __name__ == "__main__":
    run(True)

