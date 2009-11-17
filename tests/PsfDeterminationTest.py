#!/usr/bin/env python
"""
Run with:
   python PsfDeterminationTest.py
"""

import sys, os, math
from math import *

import pdb
import unittest

import eups
import lsst.utils.tests as utilsTests
import lsst.pex.harness.Clipboard as pexClipboard
import lsst.pex.policy as pexPolicy
import lsst.meas.pipeline as measPipe
import lsst.afw.detection as afwDet
import lsst.afw.image as afwImage
from lsst.pex.harness.simpleStageTester import SimpleStageTester

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class PsfDeterminationStageTestCase(unittest.TestCase):
    """A test case for SourceMeasurementStage.py"""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def clipboardIoTest(self):
        file = pexPolicy.DefaultPolicyFile("meas_pipeline", 
                "tests/sourceDetection0_policy.paf")
        detectPolicy = pexPolicy.Policy.createPolicy(file)
        
        file = pexPolicy.DefaultPolicyFile("meas_pipeline", 
                "tests/sourceMeasurement0_policy.paf")
        measurePolicy = pexPolicy.Policy.createPolicy(file)

        file = pexPolicy.DefaultPolicyFile("meas_pipeline", 
                "tests/psfDetermination0_policy.paf")
        psfPolicy = pexPolicy.Policy.createPolicy(file)
       

        tester = SimpleStageTester()
        tester.addStage(measPipe.SourceDetectionStage(detectPolicy))
        tester.addStage(measPipe.SourceMeasurementStage(measurePolicy))
        tester.addStage(measPipe.PsfDeterminationStage(psfPolicy))


        clipboard = pexClipboard.Clipboard() 
        filename = os.path.join(eups.productDir("afwdata"),
                                "CFHT", "D4", 
                                "cal-53535-i-797722_1")
        # test only a portion of the exposure to speed up testing
        bbox = afwImage.BBox(afwImage.PointI(32, 32), 512, 512)        
        testExp = afwImage.ExposureF(filename, 0, bbox)

        # test full image
        # testExp = afImage.ExposureF(filename)

        clipboard = pexClipboard.Clipboard() 
        clipboard.put(detectPolicy.get("inputKeys.exposure"), testExp)
        
        
        outWorker = tester.runWorker(clipboard)
  
        assert(outWorker.contains(psfPolicy.get("data.outputPsfKey")))
        assert(outWorker.contains(psfPolicy.get("data.outputCellSetKey")))

        del outWorker
        del testExp
        del clipboard
        del tester

        print >> sys.stderr, "at end of test"

def suite():
    """Returns a suite containing all the test cases in this module."""

    utilsTests.init()

    suites = []
    if not eups.productDir("afwdata"):
        print >> sys.stderr, "afwdata is not setting up; skipping test"
    else:
        suites += unittest.makeSuite(PsfDeterminationStageTestCase)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(exit=False):
    """Run the tests"""
    utilsTests.run(suite(), exit)

if __name__ == "__main__":
    run(True)

