#!/usr/bin/env python
"""
Run with:
   python SourceToDiaSourceStageTest.py
"""

import sys, os, math
from math import *

import pdb
import unittest
import random
import time

import lsst.utils.tests as utilsTests
import lsst.daf.base as dafBase
import lsst.pex.harness.Queue as pexQueue
import lsst.pex.harness.Clipboard as pexClipboard
import lsst.pex.policy as policy
import lsst.meas.pipeline as measPipe
import lsst.afw.detection as afwDet
import lsst.afw.image as afwImage

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class SourceToDiaSourceStageTestCase(unittest.TestCase):
    """A test case for SourceDetectionStage.py"""

    def setUp(self):
        dataPolicy1 = policy.Policy()
        dataPolicy1.add("inputKey", "sourceSet0")
        dataPolicy1.add("outputKey", "diaSourceSet0")
        dataPolicy2 = policy.Policy()
        dataPolicy2.add("inputKey", "sourceSet1")
        dataPolicy2.add("outputKey", "diaSourceSet1")
        stagePolicy = policy.Policy()
        stagePolicy.add("data", dataPolicy1)
        stagePolicy.add("data", dataPolicy2)
        stagePolicy.add("ccdWcsKey", "ccdWcs")
        stagePolicy.add("ampBBoxKey", "ampBBox")

        self.sourceSet = afwDet.SourceSet()
        self.sourceSet.append(afwDet.Source())
        self.sourceSet.append(afwDet.Source())
        self.sourceSet.append(afwDet.Source())
        self.sourceSet.append(afwDet.Source())
        self.sourceSet.append(afwDet.Source())
        

        point = afwImage.PointD(0.0, 0.0)
        wcs = afwImage.createWcs(point, point, 1, 0, 0, 1);
        ampBBox = afwImage.BBox(afwImage.PointI(0, 0), 1, 1)

        clipboard = pexClipboard.Clipboard()
        clipboard.put("sourceSet0", self.sourceSet)
        clipboard.put("sourceSet1", self.sourceSet) 
        clipboard.put("ccdWcs", wcs)
        clipboard.put("ampBBox", ampBBox)

        inQueue = pexQueue.Queue()
        inQueue.addDataset(clipboard)
        self.outQueue = pexQueue.Queue()
        
        self.stage = measPipe.SourceToDiaSourceStage(1, stagePolicy)
        self.stage.initialize(self.outQueue, inQueue)
        self.stage.setUniverseSize(1)
        self.stage.setRun("StageTest")
    
    def tearDown(self):
        del self.stage
        del self.outQueue

    def testValidClipboard(self):
        self.stage.process()
        clipboard = self.outQueue.getNextDataset()
        assert(clipboard.contains("diaSourceSet0"))
        assert(clipboard.contains("persistable_diaSourceSet0"))
        assert(clipboard.contains("diaSourceSet1"))
        assert(clipboard.contains("persistable_diaSourceSet1"))
        diaSourceSet = clipboard.get("diaSourceSet0")
        assert(diaSourceSet.size() ==self.sourceSet.size())

def suite():
    """Returns a suite containing all the test cases in this module."""

    utilsTests.init()

    suites = []
    suites += unittest.makeSuite(SourceToDiaSourceStageTestCase)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(exit=False):
    """Run the tests"""
    utilsTests.run(suite(), exit)

if __name__ == "__main__":
    run(True)

