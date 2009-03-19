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
import lsst.pex.harness.Queue as pexQueue
import lsst.pex.harness.Clipboard as pexClipboard
import lsst.pex.policy as policy
import lsst.meas.pipeline as measPipe
import lsst.afw.detection as afwDet
import lsst.afw.image as afwImage

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class AddDetectStageTestCase(unittest.TestCase):
    """A test case for SourceDetectionStage.py"""

    def setUp(self):
        self.policy1 = policy.Policy("tests/AddDetectTest1.paf")

        self.policy2 = policy.Policy("tests/AddDetectTest2.paf")
        
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
        self.outQueue1 = pexQueue.Queue()
        self.outQueue2 = pexQueue.Queue()
        
        self.stage1 = measPipe.AddAndDetectStage(1, self.policy1)
        self.stage1.initialize(self.outQueue1, inQueue1)
        self.stage1.setUniverseSize(1)
        self.stage1.setRun("SingleExposureTest")

        self.stage2 = measPipe.AddAndDetectStage(1, self.policy2)
        self.stage2.initialize(self.outQueue2, inQueue2)
        self.stage2.setUniverseSize(1)
        self.stage2.setRun("MultipleExposureTest")


    def tearDown(self):
        del self.stage1
        del self.stage2
        del self.outQueue1
        del self.outQueue2
        del self.policy1
        del self.policy2

    def testSingleInputExposure(self):
        self.stage1.process()
        clipboard = self.outQueue1.getNextDataset()
        assert(clipboard.contains(self.policy1.getString("positiveDetectionKey")))
        assert(clipboard.contains(self.policy1.getString("smoothingPsfKey")))
        assert(clipboard.contains(self.policy1.getString("exposureKey")))
       
    def testMultipleInputExposure(self):
        self.stage2.process()
        clipboard = self.outQueue2.getNextDataset()
        assert(clipboard.contains(self.policy2.getString("positiveDetectionKey")))
        assert(clipboard.contains(self.policy2.getString("smoothingPsfKey")))
        assert(clipboard.contains(self.policy2.getString("exposureKey")))

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

