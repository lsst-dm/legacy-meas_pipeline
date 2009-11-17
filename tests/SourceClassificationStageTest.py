import os, os.path
import pdb
import unittest

import eups
import lsst.utils.tests as utilsTests

from lsst.pex.harness.Clipboard import  Clipboard
import lsst.pex.harness.stage as harnessStage
from lsst.pex.harness.simpleStageTester import SimpleStageTester

import lsst.pex.policy as pexPolicy
import lsst.afw.detection as afwDet
import lsst.meas.pipeline as measPipe
from lsst.meas.pipeline.sourceClassifiers import *

class SourceClassificationStageTestCase(unittest.TestCase):
    """A test case for the SourceClassificationStage"""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testStage(self):
        file = pexPolicy.DefaultPolicyFile("meas_pipeline", 
                "sourceClassification0_policy.paf", "tests")
        policy = pexPolicy.Policy.createPolicy(file)
    
        tester = SimpleStageTester(measPipe.SourceClassificationStage(policy))

        set0 = afwDet.DiaSourceSet()
        set1 = afwDet.DiaSourceSet()
        set0.append(afwDet.DiaSource())
        set0.append(afwDet.DiaSource())
        set0.append(afwDet.DiaSource())
        set1.append(afwDet.DiaSource())
        set1.append(afwDet.DiaSource())
        set1.append(afwDet.DiaSource())

        clipboard = Clipboard()
        clipboard.put("sourceSet0", set0)
        clipboard.put("sourceSet1", set1)

        outWorker = tester.runWorker(clipboard)

        #TODO: insert assert statements here!

def suite():
    """Returns a suite containing all the test cases in this module."""

    utilsTests.init()

    suites = []
    suites += unittest.makeSuite(SourceClassificationStageTestCase)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(exit=False):
    """Run the tests"""
    utilsTests.run(suite(), exit)

if __name__ == "__main__":
    run(True)

