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
    

class PhotometricZeroPointStageParallel(harnessStage.ParallelProcessing):
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
        policyFile=pexPolicy.DefaultPolicyFile("meas_pipeline",   # package name
                                  "PhotometricZeroPointStageDictionary.paf", # default. policy
                                  "policy" # dir containing policies
                                  )
        defaultPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath())
        
        if self.policy is None:
            self.policy = defaultPolicy
        else:
            self.policy.mergeDefaults(defaultPolicy)
               
        #Setup the log
        self.log = Debug(self.log, "PhotometricZeroPointStageParallel")
        self.log.setThreshold(Log.DEBUG)
        self.log.log(Log.INFO, "Finished setup of PhotometricZeroPointStageParallel")
        

    def process(self, clipboard):
        self.log.log(Log.INFO, "Determining Photometric Zero Point")

        #Check inputs
        if clipboard is None:
            raise RuntimeError("Clipboard is empty")

        srcMatchSetKey = self.policy.get("sourceMatchSetKey")
        if not clipboard.contains(srcMatchSetKey):
            raise RuntimeError("No input SourceMatch set on clipboard")
        
        outputValueKey = self.policy.get("outputValueKey")
        outputUncKey = self.policy.get("outputUncertaintyKey")
        
        
        #Do the work
        #@FIXME. Where is this code going to be???
        zero, zeroUnc = measAstrom.calcPhotometricZeroPoint(srcMatchSetKey, log=log)
        #zero, zeroUnc = None, None

        #Save results to clipboard
        clipboard.put(self.policy.get('outputValueKey'), zero)
        clipboard.put(self.policy.get('outputUncertaintyKey'), zeroUnc)



class PhotometricZeroPointStage(harnessStage.Stage):
    """A wrapper stage that supplies the names of the classes that do the work
       Different classes are provided for serial and parallel processing
    """
    parallelClass = PhotometricZeroPointStageParallel




