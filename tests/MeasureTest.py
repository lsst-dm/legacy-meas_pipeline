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
        detectPolicy1 = policy.Policy("tests/MeasureTestDetect.paf")
        detectPolicy2 = policy.Policy("tests/AddDetectTest2.paf")
        measPolicy1 = policy.Policy("tests/MeasureTest1.paf")
        measPolicy2 = policy.Policy("tests/MeasureTest2.paf")
       
        clipboard1 = pexClipboard.Clipboard() 
        clipboard2 = pexClipboard.Clipboard()
        filename = os.path.join(eups.productDir("afwdata"),
                                "CFHT", "D4", 
                                "cal-53535-i-797722_1")
        bbox = afwImage.BBox(afwImage.PointI(32,32), 512, 512)
        exposure = afwImage.ExposureF(filename, 0,bbox)

        clipboard1.put("calibratedExposure0", exposure)
        clipboard2.put("calibratedExposure0", exposure) 
        clipboard2.put("calibratedExposure1", exposure)

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

