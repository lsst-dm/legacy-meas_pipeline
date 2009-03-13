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

class DetectStageTestCase(unittest.TestCase):
    """A test case for SourceDetectionStage.py"""

    def setUp(self):
        self.policy = policy.Policy()
        self.policy.add("exposureKey", "calibratedExposure0")
        self.policy.add("psfPolicy.algorithm", "DoubleGaussian")
        self.policy.add("psfPolicy.width", 15)
        self.policy.add("psfPolicy.height", 15)
        self.policy.add("psfPolicy.parameter", 5.0/(2*sqrt(2*log(2))))

        self.policy.add("detectionPolicy.minPixels", 1)
        self.policy.add("detectionPolicy.thresholdValue", 3.0)
        self.policy.add("detectionPolicy.thresholdType", "stdev")
        self.policy.add("detectionPolicy.thresholdPolarity", "both")
        self.policy.add("smoothingPsfKey", "psfModel")
        self.policy.add("positiveDetectionKey", "positiveFootprintSet")

        clipboard = pexClipboard.Clipboard() 
        img = afwImage.MaskedImageF(512, 512)
        img.set( 10, 1, 1)
        exp = afwImage.ExposureF(img)
        clipboard.put(self.policy.getString("exposureKey"), exp)
       
        inQueue = pexQueue.Queue()
        inQueue.addDataset(clipboard)
        self.outQueue = pexQueue.Queue()
        self.stage = measPipe.SourceDetectionStage(1, self.policy)
        self.stage.initialize(self.outQueue, inQueue)
        self.stage.setUniverseSize(1)
        self.stage.setRun("StageUnitTest")

    def tearDown(self):
        del self.stage
        del self.outQueue
        del self.policy

    def testStage(self):
        self.stage.process()
        clipboard = outQueue.getNextDataset()
        assert(clipboard.contains(self.policy.getString("positiveDetectionKey")))
        assert(clipboard.contains(self.policy.getString("negativeDetectionKey")))
        assert(clipboard.contains(self.policy.getString("smoothingPsfKey")))
        assert(clipboard.contains(self.policy.getString("exposureKey")))
        
def suite():
    """Returns a suite containing all the test cases in this module."""

    utilsTests.init()

    suites = []
    suites += unittest.makeSuite(DetectStageTestCase)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(exit=False):
    """Run the tests"""
    utilsTests.run(suite(), exit)

if __name__ == "__main__":
    run(True)

