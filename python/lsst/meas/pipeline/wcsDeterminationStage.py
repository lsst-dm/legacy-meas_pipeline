import math
import sys
import lsst.utils as utils
import os
import lsst.pex.harness.stage as harnessStage
from lsst.pex.logging import Log
import lsst.pex.policy as pexPolicy
import lsst.daf.base as dafBase
import lsst.afw.detection as afwDet
import lsst.afw.image as afwImage
import lsst.meas.astrom.net as astromNet
import lsst.pex.exceptions as exceptions
import lsst.meas.algorithms as measAlg

class WcsDeterminationStageParallel(harnessStage.ParallelProcessing):
    """Refine a WCS in an Exposure based on a list of sources

    See detection/pipeline/WcsDeterminationStageDictionary.paf for Policy file entries

    Clipboard Input:
    - a list of Exposures
    - a SourceSet
    - an initial Wcs
    - various tuning parameters

    ClipboardOutput:
    - The input Exposures with updated Wcs
    
    \todo Raise an exception instead of logging an error if the Wcs cannot be found
    (once the Wcs determination code is known to be reliable
    \todo Found out if the astrometry.net indices are memory-mapped, and if not,
    figure out how to only the required indices.
    \todo After DC3 we should not assume the input exposure has a Wcs;
    the associated minimal data (center RA/Dec, etc.) should come in as separate metadata.
    """
    def setup(self):
        #merge defaults
        file = pexPolicy.DefaultPolicyFile("meas_pipeline",
            "WcsDeterminationStageDictionary.paf", pipeline)
        defPolicy = pexPolicy.Policy.createPolicy(
            file, file.getRepositoryPath())        
        if self.policy is None:
            self.policy = defPolicy
        else:
            self.policy.mergeDefaults(defPolicy)

        #get an GlobalAstrometrySolution
        path = os.path.join(
                utils.productDir("astrometry_net_data"),
                "metadata.paf")
        self.astromSolver = astromNet.GlobalAstrometrySolution(path)

    def process(self, clipboard):
        """Determine Wcs"""
        self.log.log(Log.INFO, "Wcs Determination Stage")

        self.log.log(Log.INFO, "Reset solver")
        self.astromSolver.reset()
        

        exposureKeyList = self.policy.getStringArray("exposureKeyList")
        
        ampBBox = clipboard.get(self.policy.getString("ampBBoxKey"))
        sourceSet = clipboard.get(self.policy.getString("sourceSetKey"))
        if isinstance(sourceSet, afwDet.PersistableSourceVector):
            sourceSet = sourceSet.getSources()
        
        initialWcs = clipboard.get(exposureKeyList[0]).getWcs().clone()
        
        # Shift WCS from amp coordinates to CCD coordinates
        # Use first Exposure's WCS as the initial guess
        initialWcs.shiftReferencePixel(ampBBox.getX0(), ampBBox.getY0())

        self.log.log(Log.INFO, "Determine Wcs")
        wcs = self.determineWcs(sourceSet, initialWcs, self.policy)
        self.log.log(Log.INFO, wcs.getFitsMetadata().toString())
        
        #output the ccd WCS
        clipboard.put(self.policy.getString("outputCcdWcsKey", wcs))

        # Shift WCS from CCD coordinates to amp coordinates
        wcs.shiftReferencePixel(-ampBBox.getX0(), -ampBBox.getY0())

        # Update exposures
        for exposureKey in exposureKeyList:
            exposure = clipboard.get(exposureKey)
            exposure.setWcs(wcs.clone())

    def determineWcs(self, sourceSet, initialWcs, policy):
        """Determine Wcs of an Exposure given a SourceSet and an initial Wcs
        """
        # select sufficiently bright sources that are not flagged
        fluxLimit = self.policy.getDouble("fluxLimit")
        pixelScaleRangeFactor = self.policy.getDouble("pixelScaleRangeFactor")


        # Set parameters
        allowDistortion = self.policy.getBool("allowDistortion")
        matchThreshold = self.policy.getDouble("matchThreshold")
        self.astromSolver.allowDistortion(allowDistortion)
        self.astromSolver.setMatchThreshold(matchThreshold)

        # Shouldn't this be on the clipboard instead? But in any case,
        # we need it to determine best guess RA/Dec of center of CCD
        ccdWidth = self.policy.getInt("ccdDimensions.width")
        ccdHeight = self.policy.getInt("ccdDimensions.height")
        
        wcsSourceSet = afwDet.SourceSet()
        for source in sourceSet:
            if source.getPsfFlux() >= fluxLimit and \
                source.getFlagForDetection() == measAlg.Flags.BINNED1:
                wcsSourceSet.append(source)
        
        self.log.log(Log.INFO, "Using %s sources with flux > %s; initial list had %s sources" % \
            (len(wcsSourceSet), fluxLimit, len(sourceSet)))
        self.astromSolver.setStarlist(wcsSourceSet)

        if self.policy.exists("brightestNStars"):
            num = self.policy.getInt("brightestNStars")
            self.log.log(Log.INFO,
                    "Setting number of stars in solver to %i" %(num,))
            if num > wcsSourceSet.size():
                num = wcsSourceSet.size()
                self.log.log(Log.INFO, "Reducing to actual number: %i" % (num,))
            self.astromSolver.setNumBrightObjects(num)

        # find RA/Dec of center of image (need not be exact)
        ccdCtrPos = afwImage.PointD(
            afwImage.indexToPosition(ccdWidth / 2),
            afwImage.indexToPosition(ccdHeight / 2),
        )
        # Use RA/decl of WCS origin, not CCD center, as it seems to lead to
        # faster solutions.  TODO: figure out what the right input is.
        # predRaDecCtr = initialWcs.xyToRaDec(ccdCtrPos)
        predRaDecCtr = initialWcs.getOriginRaDec()
        self.log.log(Log.INFO, "RA/Dec at initial WCS origin = %s, %s deg" % \
            (predRaDecCtr.getX(), predRaDecCtr.getY()))
        
        # Determinate predicted image scale in arcseconds/pixel
        pixelAreaDegSq = initialWcs.pixArea(ccdCtrPos) # in degrees^2
        imageScale = math.sqrt(pixelAreaDegSq) * 3600.0
        self.log.log(Log.INFO, "Predicted image scale = %s arcsec/pixel" % (imageScale,))
    
        minImageScale = imageScale / pixelScaleRangeFactor
        maxImageScale = imageScale * pixelScaleRangeFactor
        self.astromSolver.setMinimumImageScale(minImageScale)
        self.astromSolver.setMaximumImageScale(maxImageScale)
        self.log.log(Log.INFO, "Set image scale min=%s; max=%s" % (minImageScale, maxImageScale))
    
        if True: # set True once you trust the Wcs isFlipped function or remove the conditional
            if initialWcs.isFlipped():
                self.log.log(Log.INFO, "Set flipped parity")
                self.astromSolver.setParity(astromNet.FLIPPED_PARITY)
            else:
                self.log.log(Log.INFO, "Set normal parity")
                self.astromSolver.setParity(astromNet.NORMAL_PARITY)
        else:
            self.log.log(Log.INFO, "Set unknown parity")
            self.astromSolver.setParity(astromNet.UNKNOWN_PARITY)

        try:
            solved = self.astromSolver.solve(predRaDecCtr.getX(), predRaDecCtr.getY())
        except exceptions.LsstCppException:
            err= sys.exc_info()[1]
            self.log.log(Log.WARN, err.message.what())
            solved = False
            
        if solved:
            self.log.log(Log.WARN, "Found new wcs")
            return self.astromSolver.getDistortedWcs()
        else:
            self.log.log(Log.WARN, "Failed to find WCS solution; leaving raw Wcs unchanged")
            return initialWcs

class WcsDeterminationStage(harnessStage.Stage):
    parallelClas = WcsDeterminationStageParallel
