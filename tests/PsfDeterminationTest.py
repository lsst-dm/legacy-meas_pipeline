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
        pass

    def tearDown(self):
        pass

    def testDc3PipePolicies(self):
        ipsdDir = os.path.join(eups.productDir("ctrl_dc3pipe"),
                            "pipeline", "IPSD")
        policyPath = os.path.join(ipsdDir, "sourceDetection0_policy.paf")
        policyFile = policy.PolicyFile(policyPath)
        detectPolicy = policy.Policy(policyFile)

        policyPath = os.path.join(ipsdDir, "sourceMeasurement0_policy.paf")
        policyFile = policy.PolicyFile(policyPath)
        measurePolicy = policy.Policy(policyFile)
       
        policyPath = os.path.join(ipsdDir, "psfDetermination0_policy.paf")
        policyFile = policy.PolicyFile(policyPath)
        psfPolicy = policy.Policy(policyFile)
       
        clipboard = pexClipboard.Clipboard() 
        filename = os.path.join(eups.productDir("afwdata"),
                                "CFHT", "D4", 
                                "cal-53535-i-797722_1")
        loadExp = afwImage.ExposureF(filename)
        #test only a portion of the exposure to speed up testing
        bbox = afwImage.BBox(afwImage.PointI(32, 2), 512, 512)
        testExp = afwImage.ExposureF(loadExp, bbox)

        clipboard = pexClipboard.Clipboard() 
        clipboard.put(detectPolicy.get('exposureKey'), testExp)

        inQueue = pexQueue.Queue() 
        inQueue.addDataset(clipboard)       
        detectToMeasureQueue = pexQueue.Queue()
        measureToPsfQueue = pexQueue.Queue()       
        outQueue = pexQueue.Queue()
        
        detectStage = measPipe.SourceDetectionStage(0, detectPolicy)
        detectStage.initialize(detectToMeasureQueue, inQueue)
        detectStage.setUniverseSize(1)
        detectStage.setRun("psfDeterminationTest")

        measureStage = measPipe.SourceMeasurementStage(1, measurePolicy)
        measureStage.initialize(measureToPsfQueue, detectToMeasureQueue)
        measureStage.setUniverseSize(1)
        measureStage.setRun("psfDeterminationTest")

        psfStage = measPipe.PsfDeterminationStage(2, psfPolicy)
        psfStage.initialize(outQueue, measureToPsfQueue)
        psfStage.setUniverseSize(1)
        psfStage.setRun("psfDeterminationTest")

        detectStage.process()
        measureStage.process()
        psfStage.process()

        clipboard = outQueue.getNextDataset()        
        assert(clipboard.contains(psfPolicy.getString('data.outputPsfKey')))
        assert(clipboard.contains(psfPolicy.getString('data.outputCellSetKey')))

        del detectStage
        del measureStage
        del psfStage
        del inQueue
        del detectToMeasureQueue
        del measureToPsfQueue
        del outQueue
        del clipboard
        del loadExp
        del testExp

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

