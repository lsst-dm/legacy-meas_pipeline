#!/usr/bin/env python
"""
Run with:
   python DetectTest.py
"""

import sys, os, math
from math import *

import pdb
import unittest

import eups
import lsst.utils.tests as utilsTests
import lsst.pex.harness.Clipboard as pexClipboard
import lsst.pex.policy as pexPolicy
from lsst.pex.logging import Trace
import lsst.meas.pipeline as measPipe
import lsst.afw.detection as afwDet
import lsst.afw.image as afwImage

from lsst.pex.harness.simpleStageTester import SimpleStageTester

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class DetectStageTestCase(unittest.TestCase):
    """A test case for SourceDetectionStage.py"""

    def setUp(self):
        filename = os.path.join(eups.productDir("afwdata"),
                                "CFHT", "D4", 
                                "cal-53535-i-797722_1")
        bbox = afwImage.BBox(afwImage.PointI(32,32), 512, 512)
        self.exposure = afwImage.ExposureF(filename, 0,bbox)        

    def tearDown(self):
        del self.exposure        

    def testSingleExposure(self):
        file = pexPolicy.DefaultPolicyFile("meas_pipeline", 
                "sourceDetection0_policy.paf", "tests")
        policy = pexPolicy.Policy.createPolicy(file)

        stage = measPipe.SourceDetectionStage(policy)
        tester = SimpleStageTester(stage)

        clipboard = pexClipboard.Clipboard()         
        clipboard.put(policy.get("inputKeys.exposure"), self.exposure)

        outWorker = tester.runWorker(clipboard)

        outPolicy = policy.getPolicy("outputKeys")
        detectionKey = outPolicy.get("positiveDetection")
        assert(outWorker.contains(detectionKey))
        detectionSet = outWorker.get(detectionKey)
        fpList = detectionSet.getFootprints()
        assert(fpList.size() > 0)     
        exposureKey = outPolicy.getString("backgroundSubtractedExposure")
        assert(outWorker.contains(exposureKey))
        assert(outWorker.contains(outPolicy.getString("background")))
        assert(outWorker.contains(outPolicy.getString("psf")))
    
def suite():
    """Returns a suite containing all the test cases in this module."""

    utilsTests.init()

    suites = []

    if not eups.productDir("afwdata"):
        print >> sys.stderr, "afwdata is not setting up; skipping test"
    else:        
        suites += unittest.makeSuite(DetectStageTestCase)

    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(exit=False):
    """Run the tests"""
    utilsTests.run(suite(), exit)

if __name__ == "__main__":
    run(True)

