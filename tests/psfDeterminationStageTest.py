#!/usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#

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
import lsst.afw.geom as afwGeom
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
        bbox = afwGeom.Box2I(afwGeom.Point2I(32, 32), afwGeom.Extent2I(1024, 1024))

        self.exposure =  afwImage.ExposureF(filename, 0, bbox, afwImage.LOCAL)

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
                                                                           "psfDetermination_stagePolicy.paf",
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
