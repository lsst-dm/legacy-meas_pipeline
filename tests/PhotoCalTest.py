#!/usr/bin/env python
"""
Run with:
   python DetectTest.py

    This test isn't written yet, this is just boilerplate.
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


from lsst.pex.harness.simpleStageTester import SimpleStageTester

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

def readSourceSet(fileName):
    fd = open(fileName, "r")

    sourceSet = afwDet.SourceSet()
    lineno = 0
    for line in fd.readlines():
        lineno += 1
        try:
            id, x, y, ra, dec, cts, flags = line.split()
        except Exception, e:
            print "Line %d: %s: %s" % (lineno, e, line),

        s = afwDet.Source()
        sourceSet.append(s)

        s.setId(int(id))
        s.setFlagForDetection(int(flags))
        s.setRa(float(ra))
        s.setXAstrom(float(x))
        s.setYAstrom(float(y))
        s.setDec(float(dec))
        s.setPsfFlux(float(cts))

    return sourceSet

class PhotoCalStageTestCase(unittest.TestCase):
    """A test case for PhotoCalStage.py"""

    def setUp(self):
        ##Load sample input from disk
        path = os.path.join(eups.productDir("meas_pipeline"), "tests")
        srcSet = readSourceSet(os.path.join(path, "v695833-e0-c000.xy.txt"))
        
        #Make a copy, with different fluxes.
        #The exact choice doesn't matter ,we just want to make sure the code returns an answer
        catSet = []
        for s in srcSet:
            s1 = afwDet.Source(s)
            s1.setPsfFlux(s1.getPsfFlux()*.281)
            catSet.append(s1)
            
        #Make a SourceMatch object
        maxDist = 1/3600. #matches must be this close together
        srcMatchSet = afwDet.matchXy(catSet, srcSet, maxDist)
        
        #Put them on the clipboard
        filename = pexPolicy.DefaultPolicyFile("meas_pipeline", 
                      "PhotoCalStageDictionary.paf", "policy")
        self.policy = pexPolicy.Policy.createPolicy(filename)

        self.clipboard = pexClipboard.Clipboard()         
        self.clipboard.put(self.policy.get("sourceMatchSetKey"), srcMatchSet)

        
    def tearDown(self):
        del self.policy
        
    def testSingleExposure(self):
        #Run the stage
        stage = measPipe.PhotoCalStage(self.policy)
        tester = SimpleStageTester(stage)
        outWorker = tester.runWorker(self.clipboard)

        #Check for output parameters
        magKey = self.policy.get("outputValueKey")
        assert(outWorker.contains(magKey))


    
def suite():
    """Returns a suite containing all the test cases in this module."""

    utilsTests.init()

    suites = []

    if not eups.productDir("astrometry_net_data"):
        print >> sys.stderr, "Unable to test WcsDeterminationStage as astrometry_net_data is not setup"
    else:
        suites += unittest.makeSuite(PhotoCalStageTestCase)

    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(exit=False):
    """Run the tests"""
    utilsTests.run(suite(), exit)

if __name__ == "__main__":
    run(True)
