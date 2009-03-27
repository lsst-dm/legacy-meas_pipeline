import glob
import math
import sys
from lsst.pex.harness.Stage import Stage
from lsst.pex.logging import Log
from lsst.pex.policy import Policy
# import lsst.pex.harness.Utils
import lsst.daf.base as dafBase
import lsst.afw.detection as afwDet
import lsst.afw.image as afwImage
import lsst.meas.astrom.net as astromNet
import lsst.pex.exceptions as exceptions

class WcsDeterminationStage(Stage):
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
    def initialize(self, outQueue, inQueue):
        # call base version
        Stage.initialize(self, outQueue, inQueue)

        # initialize a log
        self.log = Log(Log.getDefaultLog(), "lsst.meas.pipeline.WcsDeterminationStage")

        # Do nothing else in the master

        if self.getRank() == -1:
            return

        path = self._policy.getString("astrometryIndexMetadata")
        self.astromSolver = astromNet.GlobalAstrometrySolution(path)

    def process(self):
        """Determine Wcs"""
        self.log.log(Log.INFO, "Wcs Determination Stage")

        clipboard = self.inputQueue.getNextDataset()

        exposureKeyList = self._policy.getStringArray("exposureKeyList")
        sourceSetKey = self._policy.getString("sourceSetKey")
        ampBBoxKey = self._policy.getString("ampBBoxKey")
        outputWcsKey = self._policy.getString("outputCcdWcsKey")

        self.fluxLimit = self._policy.getDouble("fluxLimit")
        self.pixelScaleRangeFactor = self._policy.getDouble("pixelScaleRangeFactor")
        self.log.log(Log.INFO, "Reset solver")
        self.astromSolver.reset()

        # Set parameters
        allowDistortion = self._policy.getBool("allowDistortion")
        matchThreshold = self._policy.getDouble("matchThreshold")
        self.astromSolver.allowDistortion(allowDistortion)
        self.astromSolver.setMatchThreshold(matchThreshold)

        # Shouldn't this be on the clipboard instead? But in any case,
        # we need it to determine best guess RA/Dec of center of CCD
        self.ccdWidth = self._policy.getInt("ccdDimensions.width")
        self.ccdHeight = self._policy.getInt("ccdDimensions.height")

        sourceSet = clipboard.get(sourceSetKey)
        if isinstance(sourceSet, afwDet.PersistableSourceVector):
            sourceSet = sourceSet.getSources()
        
        initialWcs = clipboard.get(exposureKeyList[0]).getWcs().clone()
        ampBBox = clipboard.get(ampBBoxKey)
        
        # Shift WCS from amp coordinates to CCD coordinates
        # Use first Exposure's WCS as the initial guess
        initialWcs.shiftReferencePixel(ampBBox.getX0(), ampBBox.getY0())

        self.log.log(Log.INFO, "Determine Wcs")
        wcs = self.determineWcs(sourceSet, initialWcs)

        clipboard.put(outputWcsKey, wcs)

        # Shift WCS from CCD coordinates to amp coordinates
        wcs.shiftReferencePixel(-ampBBox.getX0(), -ampBBox.getY0())

        # Update exposures
        for exposureKey in exposureKeyList:
            exposure = clipboard.get(exposureKey)
            exposure.setWcs(wcs.clone())

        self.outputQueue.addDataset(clipboard)
    
    def determineWcs(self, sourceSet, initialWcs):
        """Determine Wcs of an Exposure given a SourceSet and an initial Wcs
        """
        # select sufficiently bright sources that are not flagged
        wcsSourceSet = afwDet.SourceSet()
        for source in sourceSet:
            if source.getPsfFlux() >= self.fluxLimit and \
                source.getFlagForDetection() == 0:
                wcsSourceSet.append(source)
        
        self.log.log(Log.INFO, "Using %s sources with flux > %s; initial list had %s sources" % \
            (len(wcsSourceSet), self.fluxLimit, len(sourceSet)))
        self.astromSolver.setStarlist(wcsSourceSet)

        # find RA/Dec of center of image (need not be exact)
        ccdCtrPos = afwImage.PointD(
            afwImage.indexToPosition(self.ccdWidth / 2),
            afwImage.indexToPosition(self.ccdHeight / 2),
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
    
        minImageScale = imageScale / self.pixelScaleRangeFactor
        maxImageScale = imageScale * self.pixelScaleRangeFactor
        self.astromSolver.setMinimumImageScale(minImageScale)
        self.astromSolver.setMaximumImageScale(maxImageScale)
        self.log.log(Log.INFO, "Set image scale min=%s; max=%s" % (minImageScale, maxImageScale))
    
        if False: # set True once you trust the Wcs isFlipped function or remove the conditional
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
            return self.astromSolver.getDistortedWcs()
        else:
            self.log.log(Log.WARN, "Failed to find WCS solution; leaving raw Wcs unchanged")
            return initialWcs
