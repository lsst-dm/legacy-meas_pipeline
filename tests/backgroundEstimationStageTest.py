#!/usr/bin/env python
"""
Run with:
   python backgroundEstimationStageTest.py
or
   python
   >>> import backgroundEstimationStageTest
   >>> backgroundEstimationStageTest.run()
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
import lsst.afw.image as afwImage
import lsst.afw.display.ds9 as ds9

from lsst.pex.harness.simpleStageTester import SimpleStageTester

try:
    type(display)
except NameError:
    display = False

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class BackgroundEstimationStageTestCase(unittest.TestCase):
    """A test case for BackgroundEstimationStage.py"""

    def setUp(self):
        filename = os.path.join(eups.productDir("afwdata"), "CFHT", "D4", "cal-53535-i-797722_1")
        bbox = afwImage.BBox(afwImage.PointI(32,32), 512, 512)
        self.exposure = afwImage.ExposureF(filename, 0,bbox)        

    def tearDown(self):
        del self.exposure        

    def testSingleExposure(self):
        
        file = pexPolicy.DefaultPolicyFile("meas_pipeline", 
                                           "backgroundEstimation_policy.paf", "tests")
        policy = pexPolicy.Policy.createPolicy(file)
        #
        # It'd be better use use the default dictionary for the stage,
        # BackgroundEstimationStageDictionary.paf, but this doesn't seem to work.  I (RHL) suspect that this
        # because of #872, but I can't be sure.  For now, leave the copies of the defaults in
        # backgroundEstimation_policy.paf.
        #
        # We pull backgroundDictionary from meas/utils/policy explicitly; this is a different problem
        # related to dictionaries being unable to load defaults from other packages; #1035
        #
        dfile = pexPolicy.DefaultPolicyFile("meas_pipeline", 
                                            "BackgroundEstimationStageDictionary.paf", "policy")
        
        defpolicy = pexPolicy.Policy.createPolicy(dfile, dfile.getRepositoryPath())
        policy.mergeDefaults(defpolicy) # does nothing

        if True:                        # workaround #1035
            dfile = pexPolicy.DefaultPolicyFile("meas_utils", 
                                                "BackgroundDictionary.paf", "policy")
            
            defpolicy = pexPolicy.Policy.createPolicy(dfile, dfile.getRepositoryPath())
            tmp = pexPolicy.Policy()
            tmp.mergeDefaults(defpolicy)
            policy.add("backgroundPolicy", tmp)

        stage = measPipe.BackgroundEstimationStage(policy)
        tester = SimpleStageTester(stage)

        clipboard = pexClipboard.Clipboard()         
        clipboard.put(policy.get("inputKeys.exposure"), self.exposure)

        if display:
            ds9.mtv(self.exposure, frame=0, title="Input")
        #
        # Do the work
        #
        outWorker = tester.runWorker(clipboard)

        outPolicy = policy.getPolicy("outputKeys")
        assert(outWorker.contains(outPolicy.getString("backgroundSubtractedExposure")))
        assert(outWorker.contains(outPolicy.getString("background")))

        if display:
            ds9.mtv(outWorker.get(outPolicy.getString("backgroundSubtractedExposure")),
                    frame=1, title="Subtracted")

def suite():
    """Returns a suite containing all the test cases in this module."""

    utilsTests.init()

    suites = []

    if not eups.productDir("afwdata"):
        print >> sys.stderr, "afwdata is not setting up; skipping test"
    else:        
        suites += unittest.makeSuite(BackgroundEstimationStageTestCase)

    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(exit=False):
    """Run the tests"""
    utilsTests.run(suite(), exit)

if __name__ == "__main__":
    run(True)

