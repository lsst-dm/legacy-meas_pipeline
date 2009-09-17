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

import eups
import lsst.utils.tests as utilsTests
import lsst.pex.harness.Queue as pexQueue
import lsst.pex.harness.Clipboard as pexClipboard
import lsst.pex.policy as policy
from lsst.pex.logging import Trace
import lsst.meas.pipeline as measPipe
import lsst.afw.detection as afwDet
import lsst.afw.image as afwImage

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class DetectStageTestCase(unittest.TestCase):
    """A test case for SourceDetectionStage.py"""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testDc3PipePolicy(self):
        if not eups.productDir("afwdata"):
            print >> sys.stderr, "afwdata is not setting up; skipping test"
            return

        ipsdDir = os.path.join(eups.productDir("meas_pipeline"),\
                            "tests")
        policyPath = os.path.join(ipsdDir, "sourceDetection0_policy.paf")
        policyFile = policy.PolicyFile(policyPath)
        stagePolicy = policy.Policy(policyFile)
        
        clipboard = pexClipboard.Clipboard() 
        filename = os.path.join(eups.productDir("afwdata"),
                                "CFHT", "D4", 
                                "cal-53535-i-797722_1")
        bbox = afwImage.BBox(afwImage.PointI(32,32), 512, 512)
        exposure = afwImage.ExposureF(filename, 0,bbox)
        clipboard.put(stagePolicy.getString("exposureKey"), exposure)
       
        inQueue = pexQueue.Queue()
        inQueue.addDataset(clipboard)
        outQueue = pexQueue.Queue()
        stage = measPipe.SourceDetectionStage(1, stagePolicy)
        stage.initialize(outQueue, inQueue)
        stage.setUniverseSize(1)
        
        stage.process()

        clipboard = outQueue.getNextDataset()
        detectionKey = stagePolicy.getString("positiveDetectionKey")
        assert(clipboard.contains(detectionKey))
        detectionSet = clipboard.get(detectionKey)
        fpList = detectionSet.getFootprints()
        assert(fpList.size() > 0) 
        psfKey = stagePolicy.getString("psfKey")
        assert(clipboard.contains(psfKey))
        exposureKey = stagePolicy.getString("exposureKey")
        assert(clipboard.contains(exposureKey))
        
        
        del stagePolicy
        del clipboard
        del stage
        del inQueue
        del outQueue

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

