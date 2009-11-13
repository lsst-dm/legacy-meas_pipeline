#!/usr/bin/env python
"""
Run with:
   python DetectTest.py
"""

import sys, os, math
from math import *

import pdb
import unittest
import random
import time

import lsst.utils.tests as utilsTests
import lsst.pex.harness as pexHarness
from lsst.pex.harness.simpleStageTester import SimpleStageTester
import lsst.pex.harness.Clipboard as Clipboard
import lsst.pex.policy as pexPolicy
import lsst.meas.pipeline as measPipe
import lsst.afw.detection as afwDet
import lsst.afw.image as afwImage

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class AddDetectStageTestCase(unittest.TestCase):
    """A test case for AddAndDetectStage.py"""

    def setUp(self):
        img = afwImage.MaskedImageF(512, 512)
        img.set( 10, 1, 1)
        self.exposure = afwImage.ExposureF(img)
        del img

    def tearDown(self):
        del self.exposure

    def testSingleInputExposure(self):
        file = pexPolicy.DefaultPolicyFile("meas_pipeline", 
                "tests/AddDetectTest1.paf")
        policy = pexPolicy.Policy.createPolicy(file)

        stage = measPipe.AddAndDetectStage(policy)
        tester = SimpleStageTester(stage)

        tester.setDebugVerbosity(5)

        clipboard = dict(calibratedExposure0=self.exposure)
        outWorker = tester.runWorker(clipboard)
    
        assert(outWorker.contains(policy.getString("positiveDetectionKey")))        
        assert(outWorker.contains(policy.getString("psfKey")))
        assert(outWorker.contains(policy.getString("exposureKey")))
       
    def testMultipleInputExposure(self):
        file = pexPolicy.DefaultPolicyFile("meas_pipeline", 
                "tests/AddDetectTest2.paf")
        policy = pexPolicy.Policy.createPolicy(file)

        stage = measPipe.AddAndDetectStage(policy)
        tester = SimpleStageTester(stage)

        tester.setDebugVerbosity(5)

        clipboard = dict(
            calibratedExposure0=self.exposure, 
            calibratedExposure1=self.exposure
        )
        outWorker = tester.runWorker(clipboard)
    
        assert(outWorker.contains(policy.getString("positiveDetectionKey")))        
        assert(outWorker.contains(policy.getString("psfKey")))
        assert(outWorker.contains(policy.getString("exposureKey")))

def suite():
    """Returns a suite containing all the test cases in this module."""

    utilsTests.init()

    suites = []
    suites += unittest.makeSuite(AddDetectStageTestCase)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(exit=False):
    """Run the tests"""
    utilsTests.run(suite(), exit)

if __name__ == "__main__":
    run(True)

