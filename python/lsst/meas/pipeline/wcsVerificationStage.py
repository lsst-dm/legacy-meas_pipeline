#! /usr/bin/env python

import os
import sys
import math

import eups
import lsst.pex.harness as pexHarness
import lsst.pex.harness.stage as harnessStage
import lsst.pex.policy as pexPolicy
from lsst.pex.logging import Log, Debug, LogRec, Prop
from lsst.pex.exceptions import LsstCppException

import lsst.meas.astrom.sip as sip
import pdb
    

class WcsVerificationParallel(harnessStage.ParallelProcessing):
    """Compute some statistics that indicate if we did a good job computing the Wcs for an image.
    """
    
    def setup(self):
        policyFile=pexPolicy.DefaultPolicyFile("meas_pipeline",   # package name
                                  "WcsVerificationStageDictionary.paf", # default. policy
                                  "policy" # dir containing policies
                                  )
        defaultPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath())
        
        if self.policy is None:
            self.policy = defaultPolicy
        else:
            self.policy.mergeDefaults(defaultPolicy)
               
        #Setup the log
        self.log = Debug(self.log, "WcsVerificationStageParallel")
        self.log.setThreshold(Log.DEBUG)
        self.log.log(Log.INFO, "Finished setup of WcsVerificationStageParallel")
        

    def process(self, clipboard):
        self.log.log(Log.INFO, "Calculating statistics on wcs solution")

        #Get input
        if clipboard is None:
            raise RuntimeError("Clipboard is empty")

        srcMatchSetKey = self.policy.get("sourceMatchSetKey")
        if not clipboard.contains(srcMatchSetKey):
            raise RuntimeError("No input SourceMatch set on clipboard")
        srcMatchSet = clipboard.get(srcMatchSetKey)
        
        #Do the work
        outputDict = sip.sourceMatchStatistics(srcMatchSet)

        #Save results to clipboard
        outputDictKey = self.policy.get("outputDictKey")

        clipboard.put(outputDictKey, outputDict)



class WcsVerificationStage(harnessStage.Stage):
    """A wrapper stage that supplies the names of the classes that do the work
       Different classes are provided for serial and parallel processing
    """
    parallelClass = WcsVerificationParallel




