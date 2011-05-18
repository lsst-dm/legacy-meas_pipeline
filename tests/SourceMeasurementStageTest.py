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

import pdb
import unittest
import random
import time

import eups
import lsst.utils.tests as utilsTests
import lsst.pex.harness.Clipboard as pexClipboard
import lsst.pex.policy as pexPolicy
import lsst.meas.pipeline as measPipe
import lsst.afw.geom as afwGeom
import lsst.afw.image as afwImage
import lsst.afw.detection as afwDetection
import lsst.daf.persistence as dafPersist
import lsst.daf.base as dafBase
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
        bbox = afwGeom.Box2I(afwGeom.Point2I(32, 32), afwGeom.Extent2I(512, 512))
        self.exposure =  afwImage.ExposureF(filename, 0, bbox, afwImage.LOCAL)

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

        src = outClipboard.get(sources)[0]
        print src.getAstrometry(), src.getPhotometry(), src.getShape()

        psv = outClipboard.get(sources + "_persistable")
        src = psv.getSources()[0]
        print src.getAstrometry(), src.getPhotometry(), src.getShape()
        pers = dafPersist.Persistence.getPersistence(pexPolicy.Policy())
        loc = dafPersist.LogicalLocation("testSource.boost")
        dp = dafBase.PropertySet()
        dp.setLongLong("ampExposureId", 10)
        stl = dafPersist.StorageList()
        stl.append(pers.getPersistStorage("BoostStorage", loc))
        pers.persist(psv, stl, dp) 

        stl = dafPersist.StorageList()
        stl.append(pers.getRetrieveStorage("BoostStorage", loc))
        persistable = pers.unsafeRetrieve("PersistableSourceVector", stl, dp)
        res = afwDetection.PersistableSourceVector.swigConvert(persistable)

        src = res.getSources()[0]
        print src.getAstrometry(), src.getPhotometry(), src.getShape()

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
