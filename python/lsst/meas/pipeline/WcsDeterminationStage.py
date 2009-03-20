from lsst.pex.harness.Stage import Stage
from lsst.pex.logging import Log
# import lsst.pex.harness.Utils
import lsst.daf.base as dafBase
import lsst.afw.detection as afwDet
import lsst.afw.image as afwImage
import lsst.meas.astrom.net as astromNet
import lsst.pex.exceptions as exceptions
import glob
import math
import sys

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
    def __init__(self, stageId = -1, policy = None):
        # call base constructor
        Stage.__init__(self, stageId, policy)
        # initialize a log
        self.log = Log(Log.getDefaultLog(), "lsst.meas.pipeline.WcsDeterminationStage")
        self.astromSolver = astromNet.GlobalAstrometrySolution()
        #Read in the indices (i.e the files containing the positions of known asterisms
        #and add them to the astromSolver object
        astrometryIndicesGlob = self._policy.getString("astrometryIndicesGlob")
        indexPathList = glob.glob(astrometryIndicesGlob)
        for indexPath in indexPathList:
            self.log.log(Log.INFO, "Reading astrometry index file %s" % (indexPath,))
            self.astromSolver.addIndexFile(indexPath)
    
    def process(self):
        """Determine Wcs"""
        self.log.log(Log.INFO, "Wcs Determination Stage")

        clipboard = self.inputQueue.getNextDataset()
        exposureKeyList = self._policy.getStringArray("exposureKeyList")
        sourceSetKey = self._policy.getString("sourceSetKey")
        ampBBoxKey = self._policy.getString("ampBBoxKey")
        self.fluxLimit = self._policy.getDouble("fluxLimit")
        
        # Set parameters and compute Wcs
        allowDistortion = self._policy.getBool("allowDistortion")
        matchThreshold = self._policy.getDouble("matchThreshold")
        self.astromSolver.allowDistortion(allowDistortion)
        self.astromSolver.setMatchThreshold(matchThreshold)

        sourceSet = clipboard.get(sourceSetKey)

        # Shift WCS from amp coordinates to CCD coordinates
        initialWcsKey = self._policy.getString("initialWcsKey")
        ampBBox = clipboard.get(ampBBoxKey)
        initialWcs = clipboard.get(initialWcsKey)
        newWcs = initialWcs.clone()
        newWcs.shiftReferencePixel(ampBBox.getX0(), ampBBox.getY0())

        self.log.log(Log.INFO, "Determine Wcs")
        wcs = self.determineWcs(sourceSet, newWcs)

        # Shift WCS from CCD coordinates to amp coordinates
        wcs.shiftReferencePixel(-ampBBox.getX0(), -ampBBox.getY0())

        # Update exposures
        for exposureKey in exposureKeyList:
            exposure = clipboard.get(exposureKey)
            exposure.setWcs(wcs)

        self.outputQueue.addDataSet(clipboard)
    
    def determineWcs(self, sourceSet, initialWcs):
        """Determine Wcs of an Exposure given a SourceSet and an initial Wcs
        """
        # select sufficiently bright sources
        wcsSourceSet = afwDet.SourceSet()
        for source in sourceSet:
            #This test previously checked for fluxes brighter than self.fluxlimit
            #but that variable isn't defined so I insist on >0 instead.
            if source.getPsfFlux() >= 0: 
                wcsSourceSet.append(source)
        

        try:
            outWcs = self.astromSolver.solve(wcsSourceSet, initialWcs)
        except exceptions.LsstCppException:
            err= sys.exc_info()[1]
            self.log.log(Log.WARN, err.message.what())
            self.log.log(Log.WARN, "Failed to find WCS solution; leaving raw Wcs unchanged")
            outWcs = initialWcs
        
        return outWcs

