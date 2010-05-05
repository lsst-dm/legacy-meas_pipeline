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
import lsst.meas.astrom.sip.cleanBadPoints as cleanBadPoints

import pdb
    

class WcsDeterminationStageParallel(harnessStage.ParallelProcessing):
    """Validate the Wcs for an image using the astrometry.net package and calculate distortion
    coefficients
    
    Given a initial Wcs, and a list of sources (with pixel positions for each) in an image,
    pass these to the astrometry_net package to verify the result. Then calculate
    the distortion in the image and add that to the Wcs as SIP polynomials
    
    Clipboard Input:
    - an Exposure containing a Wcs
    - a SourceSet
    
    Clipboard Output
    - A wcs
    - A vector of SourceMatch objects
    """
    
    def setup(self):
        #I don't have the default policy in the correct place yet
        policyFile=pexPolicy.DefaultPolicyFile("meas_pipeline",   # package name
                                  "WcsDeterminationStageDictionary.paf", # default. policy
                                  "policy" # dir containing policies
                                  )
        defaultPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath())
        
        #The stage can be called with an optional local policy file, which overrides the defaults
        #merge defaults
        policyFile = pexPolicy.DefaultPolicyFile("meas_pipeline",
            "WcsDeterminationStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath(), True)

        if self.policy is None:
            self.policy = defaultPolicy
        else:
            self.policy.mergeDefaults(defaultPolicy)
               
        #Setup the log
        self.log = Debug(self.log, "WcsDeterminationStageParallel")
        self.log.setThreshold(Log.DEBUG)
        self.log.log(Log.INFO, "Finished setup of WcsDeterminationStageParallel")
        

    def process(self, clipboard):
        self.log.log(Log.INFO, "Determining Wcs")

        #Check inputs
        if clipboard is None:
            raise RuntimeError("Clipboard is empty")

        expKey = self.policy.get('inputExposureKey')
        if not clipboard.contains(expKey):
            raise RuntimeError("No exposure on clipboard")
        exp = clipboard.get(expKey)

        srcSetKey=self.policy.get('inputSourceSetKey')
        
        if not clipboard.contains(srcSetKey):
            raise RuntimeError("No wcsSourceSet on clipboard")
        srcSet = clipboard.get(srcSetKey)
        
        #Determine list of matching sources, and Wcs
        matchList, wcs = measAstrom.determineWcs(self.policy, exp, 
                srcSet, log=self.log)

        #Save results to clipboard
        clipboard.put(self.policy.get('outputMatchListKey'), matchList)
        clipboard.put(self.policy.get('outputWcsKey'), wcs)



class WcsDeterminationStage(harnessStage.Stage):
    """A wrapper stage that supplies the names of the classes that do the work
       Different classes are provided for serial and parallel processing
    """
    parallelClass = WcsDeterminationStageParallel




