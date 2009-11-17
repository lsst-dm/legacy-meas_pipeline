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
                "sourceDetection0_policy.paf", "tests")
        detectPolicy = pexPolicy.Policy.createPolicy(file)
        file = pexPolicy.DefaultPolicyFile("meas_pipeline", 
                "sourceMeasurement0_policy.paf", "tests")
        measurePolicy = pexPolicy.Policy.createPolicy(file)


        tester = SimpleStageTester()
        tester.addStage(measPipe.SourceDetectionStage(detectPolicy))
        tester.addStage(measPipe.SourceMeasurementStage(measurePolicy))

        clipboard = pexClipboard.Clipboard()
        clipboard.put(detectPolicy.get("inputKeys.exposure"), self.exposure)
        
        outWorker = tester.runWorker(clipboard)

        dataPolicy = measurePolicy.getPolicy("data")
        outputKey = dataPolicy.get("outputKey")
        assert(outWorker.contains(outputKey))
        assert(outWorker.contains("persistable_" + outputKey))

        del clipboard
        del outWorker

    def testMultipleInputExposure(self):
        file = pexPolicy.DefaultPolicyFile("meas_pipeline", 
                "sourceDetection1_policy.paf", "tests")
        detectPolicy = pexPolicy.Policy.createPolicy(file)
        file = pexPolicy.DefaultPolicyFile("meas_pipeline", 
                "sourceMeasurement1_policy.paf", "tests")
        measurePolicy = pexPolicy.Policy.createPolicy(file)

        tester = SimpleStageTester()
        tester.addStage(measPipe.SourceDetectionStage(detectPolicy))
        tester.addStage(measPipe.SourceMeasurementStage(measurePolicy))

        clipboard = pexClipboard.Clipboard()         
        exposureKeys = detectPolicy.getStringArray("inputKeys.exposure")
        for key in exposureKeys:
            clipboard.put(key, self.exposure)
        
        outWorker = tester.runWorker(clipboard)

        dataPolicyList = measurePolicy.getPolicyArray("data")
        for dataPolicy in dataPolicyList:
            outputKey = dataPolicy.get("outputKey")
            assert(outWorker.contains(outputKey))
            assert(outWorker.contains("persistable_" + outputKey))

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

