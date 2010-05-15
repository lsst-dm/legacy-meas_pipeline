#!/usr/bin/env python
"""
Run with:
   python MeasureTest.py
"""

import sys, os, math
from math import *

import unittest
import random
import time

import eups
import lsst.utils.tests as utilsTests
import lsst.pex.harness.Clipboard as pexClipboard
import lsst.pex.policy as pexPolicy
import lsst.meas.pipeline as measPipe
import lsst.meas.algorithms.measureSourceUtils as maUtils
import lsst.afw.image as afwImage
from lsst.pex.harness.simpleStageTester import SimpleStageTester

import lsst.afw.display.ds9 as ds9

try:
    type(display)
except NameError:
    display = False

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class PsfDeterminationStageTestCase(unittest.TestCase):
    """A test case for PsfDeterminationStage.py"""

    def setUp(self):
        filename = os.path.join(eups.productDir("afwdata"), "CFHT", "D4", "cal-53535-i-797722_1")
        bbox = afwImage.BBox(afwImage.PointI(32,32), 1024, 1024)

        self.exposure =  afwImage.ExposureF(filename, 0, bbox)

    def tearDown(self):
        del self.exposure

    def testSingleInputExposure(self):
        tester = SimpleStageTester()
        clipboard = pexClipboard.Clipboard()
        #
        policy = pexPolicy.Policy.createPolicy(pexPolicy.DefaultPolicyFile("meas_pipeline", 
                                                                           "sourceDetection0_policy.paf",
                                                                           "tests"))
        tester.addStage(measPipe.SourceDetectionStage(policy))
        clipboard.put(policy.get("inputKeys.exposure"), self.exposure)
        #
        policy = pexPolicy.Policy.createPolicy(pexPolicy.DefaultPolicyFile("meas_pipeline", 
                                                                           "sourceMeasurement0_policy.paf",
                                                                           "tests"))
        tester.addStage(measPipe.SourceMeasurementStage(policy))
        #
        policy = pexPolicy.Policy.createPolicy(pexPolicy.DefaultPolicyFile("meas_pipeline", 
                                                                           "psfDetermination_policy.paf",
                                                                           "tests"))
        tester.addStage(measPipe.PsfDeterminationStage(policy))
        
        if display:
            frame = 0
            ds9.mtv(self.exposure, frame=frame, title="Input"); frame += 1
        #
        # Do the work
        #
        outClipboard = tester.runWorker(clipboard)
        #
        # See if we got it right
        #
        if display:
            ds9.mtv(self.exposure, frame=frame, title="Detected"); frame += 1

        psf = outClipboard.get(policy.get("outputKeys.psf"))
        psfCellSet = outClipboard.get(policy.get("outputKeys.cellSet"))

        if display:
            maUtils.showPsf(psf, frame=frame); frame += 1
            maUtils.showPsfMosaic(self.exposure, psf, frame=frame); frame += 1
            mos = maUtils.showPsfCandidates(self.exposure, psfCellSet, frame=frame); frame += 1

        del clipboard
        del outClipboard

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
