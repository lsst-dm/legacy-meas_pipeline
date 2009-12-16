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
        wcs = determineWcs(self.log, self.policy, clipboard)



class WcsDeterminationStage(harnessStage.Stage):
    """A wrapper stage that supplies the names of the classes that do the work
       Different classes are provided for serial and parallel processing
    """
    parallelClass = WcsDeterminationStageParallel




def determineWcs(log, policy, clipboard):

    log.log(Log.INFO, "In determineWcs")

    #Check inputs
    if clipboard is None:
        raise RuntimeError("Clipboard is empty")
    
    expKey = policy.get('inputExposureKey')
    if not clipboard.contains(expKey):
        raise RuntimeError("No exposure on clipboard")
    exp = clipboard.get(expKey)
    
    srcSetKey=policy.get('inputSourceSetKey')
    if not clipboard.contains(srcSetKey):
        raise RuntimeError("No wcsSourceSet on clipboard")
    srcSet = clipboard.get(srcSetKey)

    outputWcsKey = policy.get('outputWcsKey')
    
    #Extract an initial guess wcs if available    
    wcsIn = exp.getWcs() #May be None
    if wcsIn is None:
        log.log(log.WARN, "No wcs found on exposure. Doing blind solve")
    
    #Setup solver
    path=os.path.join(eups.productDir("astrometry_net_data"), "metadata.paf")
    solver = astromNet.GlobalAstrometrySolution(path)
    solver.allowDistortion(policy.get('allowDistortion'))
    matchThreshold = policy.get('matchThreshold')
    solver.setMatchThreshold(matchThreshold)

    log.log(log.DEBUG, "Setting starlist")
    solver.setStarlist(srcSet)
    log.log(log.DEBUG, "Setting numBrightObj")
    solver.setNumBrightObjects(policy.get('numBrightStars'))

    #Do a blind solve if we're told to, or if we don't have an input wcs
    doBlindSolve = policy.get('blindSolve') or (wcsIn is None)
    
    log.log(log.DEBUG, "Solving")
    if doBlindSolve or True:
        isSolved = solver.solve()
    else:
        isSolved = solver.solve(wcsIn)

    log.log(log.DEBUG, "Finished Solve step")
    if isSolved == False:
        log.log(log.WARN, "No solution found, using input Wcs")
        clipboard.put(outputWcsKey, wcsIn)
        return
    
    #
    #Do sip corrections
    #
    #First obtain the catalogue-listed positions of stars
    log.log(log.DEBUG, "Determining match objects")
    linearWcs = solver.getWcs()
    imgSizeInArcsec = getImageSizeInArcsec(srcSet, linearWcs)
    cat = solver.getCatalogue(2*imgSizeInArcsec) #Catalogue of nearby stars
    
    #Now generate a list of matching objects, and store on the clipboard for later use
    distInArcsec = policy.get('distanceForCatalogueMatchinArcsec')
    cleanParam = policy.get('cleaningParameter')

    matchList = matchSrcAndCatalogue(cat=cat, img=srcSet, wcs=linearWcs, 
        distInArcsec=distInArcsec, cleanParam=cleanParam)
            
    if len(matchList) == 0:
        log.log(Log.WARN, "No matches found between input source and catalogue.")
        log.log(Log.WARN, "Something in wrong. Defaulting to input wcs")
        clipboard.put(outputWcsKey, wcsIn)
        return
        
    log.log(Log.INFO, "%i objects out of %i match sources listed in catalogue" %(len(matchList), len(srcSet)))
        
    clipboard.put(policy.get('outputMatchListKey'), matchList)
    
    #Now create a wcs with SIP polynomials
    maxScatter = policy.get('wcsToleranceInArcsec')
    maxSipOrder= policy.get('maxSipOrder')
    print maxScatter, maxSipOrder
    sipObject = sip.CreateWcsWithSip(matchList, linearWcs, maxScatter, maxSipOrder)
    clipboard.put(outputWcsKey, sipObject.getNewWcs())
    
    log.log(Log.INFO, "Using %i th order SIP polynomial. Scatter is %.g arcsec" \
        %(sipObject.getOrder(), sipObject.getScatterInArcsec()))

    log.log(log.DEBUG, "Finish process")
    solver.reset()
    
    
def getImageSizeInArcsec(srcSet, wcs):
    """ Get the approximate size of the image in arcseconds
    
    Input: 
    srcSet List of detected objects in the image (with pixel positions)
    wcs    Wcs converts pixel positions to ra/dec
    
    """
    xfunc = lambda x: x.getXAstrom()
    yfunc = lambda x: x.getYAstrom()
    
    x = map(xfunc, srcSet)
    y = map(yfunc, srcSet)
    
    minx = min(x)
    maxx = max(x)
    miny = min(y)
    maxy = max(y)
    
    llc = wcs.xyToRaDec(minx, miny)
    urc = wcs.xyToRaDec(maxx, maxy)
    
    deltaRa = urc[0]-llc[0]
    deltaDec = urc[1] - llc[1]
    
    #Approximately right
    dist = math.sqrt(deltaRa**2 + deltaDec**2)
    return dist*3600  #arcsec


def matchSrcAndCatalogue(cat=None, img=None, wcs=None, distInArcsec=1.0, cleanParam=3):
    """Given an input catalogue, match a list of objects in an image, given
    their x,y position and a wcs solution.
    
    Return: A list of x, y, dx and dy. Each element of the list is itself a list
    """
    
    if cat is None:
        raise RuntimeError("Catalogue list is not set")
    if img is None:
        raise RuntimeError("Image list is not set")
    if wcs is None:
        raise RuntimeError("wcs is not set")
    
        
    matcher = sip.MatchSrcToCatalogue(cat, img, wcs, distInArcsec)    
    matchList = matcher.getMatches()
    

    if matchList is None:
        raise RuntimeError("No matches found between image and catalogue")

    matchList = cleanBadPoints.clean(matchList, wcs, nsigma=cleanParam)
    return matchList


