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
import lsst.afw.image as afwImage
from lsst.pex.harness.simpleStageTester import SimpleStageTester

import lsst.afw.display.ds9 as ds9

try:
    type(display)
except NameError:
    display = False

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class SourceMeasurementStageTestCase(unittest.TestCase):
    """A test case for SourceMeasurementStage.py"""

    def setUp(self):
        filename = os.path.join(eups.productDir("afwdata"), "CFHT", "D4", "cal-53535-i-797722_1")
        bbox = afwImage.BBox(afwImage.PointI(32,32), 512, 512)
        self.exposure =  afwImage.ExposureF(filename, 0, bbox)

    def tearDown(self):
        del self.exposure

    def testSingleInputExposure(self):
        polFile = pexPolicy.DefaultPolicyFile("meas_pipeline", 
                                              "sourceDetection0_policy.paf", "tests")
        detectPolicy = pexPolicy.Policy.createPolicy(polFile)

        polFile = pexPolicy.DefaultPolicyFile("meas_pipeline", 
                                              "sourceMeasurement0_policy.paf", "tests")
        measurePolicy = pexPolicy.Policy.createPolicy(polFile)

        tester = SimpleStageTester()
        tester.addStage(measPipe.SourceDetectionStage(detectPolicy))
        tester.addStage(measPipe.SourceMeasurementStage(measurePolicy))

        clipboard = pexClipboard.Clipboard()
        clipboard.put(detectPolicy.get("inputKeys.exposure"), self.exposure)
        
        if display:
            ds9.mtv(self.exposure, frame=0, title="Input")
        #
        # Do the work
        #
        outClipboard = tester.runWorker(clipboard)
        #
        # See if we got it right
        #
        if display:
            ds9.mtv(outClipboard.get(detectPolicy.get("outputKeys.backgroundSubtractedExposure")),
                    frame=1, title="Detected")

        sources = measurePolicy.get("outputKeys.sources")
        assert(outClipboard.contains(sources))
        if False:
            assert(outClipboard.contains("persistable_" + sources))

        del clipboard
        del outClipboard

def suite():
    """Returns a suite containing all the test cases in this module."""

    utilsTests.init()

    suites = []

    if not eups.productDir("afwdata"):
        print >> sys.stderr, "afwdata is not setting up; skipping test"
    else:
        suites += unittest.makeSuite(SourceMeasurementStageTestCase)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(exit=False):
    """Run the tests"""
    utilsTests.run(suite(), exit)

if __name__ == "__main__":
    run(True)
