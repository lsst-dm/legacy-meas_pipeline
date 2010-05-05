#! /usr/bin/env python

import os
import sys
import math

import eups
import lsst.pex.harness as pexHarness
import lsst.pex.harness.stage as harnessStage
from lsst.pex.harness.simpleStageTester import SimpleStageTester
import lsst.pex.policy as pexPolicy
from lsst.pex.logging import Log, Debug, LogRec, Prop
from lsst.pex.exceptions import LsstCppException
import lsst.afw.image as afwImg

import lsst.meas.astrom as measAstrom
import lsst.meas.astrom.net as astromNet
import lsst.meas.astrom.sip as sip
import lsst.meas.photocal as photocal
import lsst.meas.astrom.sip.cleanBadPoints as cleanBadPoints

import pdb
    

class PhotoCalStageParallel(harnessStage.ParallelProcessing):
    """Calculate the magnitude zero point for a SourceSet for an image that
    has been matched to a corresponding SourceSet for a catalogue
    """
    
    def setup(self):
        policyFile=pexPolicy.DefaultPolicyFile("meas_pipeline",   # package name
                                  "PhotoCalStageDictionary.paf", # default. policy
                                  "policy" # dir containing policies
                                  )
        defaultPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath())
        
        if self.policy is None:
            self.policy = defaultPolicy
        else:
            self.policy.mergeDefaults(defaultPolicy)
               
        #Setup the log
        self.log = Debug(self.log, "PhotoCalStageParallel")
        self.log.setThreshold(Log.DEBUG)
        self.log.log(Log.INFO, "Finished setup of PhotoCalStageParallel")
        

    def process(self, clipboard):
        self.log.log(Log.INFO, "Determining Photometric Zero Point")

        #Check inputs
        if clipboard is None:
            raise RuntimeError("Clipboard is empty")

        srcMatchSetKey = self.policy.get("sourceMatchSetKey")
        if not clipboard.contains(srcMatchSetKey):
            raise RuntimeError("No input SourceMatch set on clipboard")
        srcMatchSet = clipboard.get(srcMatchSetKey)            
        
       
        
        #Do the work
        magObj = photocal.calcPhotoCal(srcMatchSet, log=self.log)

        #Save results to clipboard
        outputValueKey = self.policy.get("outputValueKey")
        clipboard.put(outputValueKey, magObj)



class PhotoCalStage(harnessStage.Stage):
    """A wrapper stage that supplies the names of the classes that do the work
       Different classes are provided for serial and parallel processing
    """
    parallelClass = PhotoCalStageParallel



