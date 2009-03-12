from lsst.pex.harness.Stage import Stage

from lsst.pex.logging import Log

# import lsst.pex.harness.Utils
import lsst.daf.base as dafBase
import lsst.afw.detection as afwDet
import lsst.afw.image as afwImgage
import lsst.meas.astrom.net as astromNet

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
        lsst.pex.harness.Stage.Stage.__init__(self, stageId, policy)
        # initialize a log
        self.log = Log(Log.getDefaultLog(), "lsst.meas.pipeline.WcsDeterminationStage")
        self.astromSolver = None
    
    def process(self):
        """Determine Wcs"""
        self.log.log(Log.INFO, "Wcs Determination Stage")

        clipboard = self.inputQueue.getNextDataset()
        exposureNameList = self._policy.getStringArray("exposureNames")
        sourceSetName = self._policy.getString("sourceSetName")
        initialWcsName = self._policy.getString("initialWcsName")
        self.fluxLimit = self._policy.getDouble("fluxLimit")
        if len(exposureNameList) != len(sourceSetName):
            raise RuntimeError("Numer of exposureNames = %s != number of sourceSetNames %s" %
                (len(exposureNames), len(sourceSetNames)))
        
        # Read in astrometry.net indices, if not already done
        # todo: only read in required portions or mmap the files
        if self.astromSolver == None:
            self.astromSolver = astromNet.GlobalAstrometrySolution()
            #Read in the indices (i.e the files containing the positions of known asterisms
            #and add them to the astromSolver object
            astrometryIndicesGlob = self._policy.getString("astrometryIndicesGlob")
            indexPathList = glob.glob(astrometryIndicesGlob)
            for indexPath in indexPathList:
                self.log.log(Log.INFO, "Reading astrometry index file %s" % (indexPath,))
                self.astromSolver.addIndexFile(indexPath)

        # Set parameters and compute Wcs
        allowDistortion = self._policy.getBoolean("allowDistortion")
        matchThreshold = self._policy.getDouble("matchThreshold")
        self.astromSolver.allowDistortion(allowDistortion)
        self.astromSolver.setMatchThreshold(matchThreshold)
        sourceSet = clipboard.get(sourceSetName)
        initialWcs = clipboard.get(initialWcsName)
        self.log.log(Log.INFO, "Determine Wcs")
        wcs = self.determineWcs(sourceSet, initialWcs)

        # Update exposures
        for exposureName in exposureNameList:
            exposure = clipboard.get(exposureName)
            exposure.setWcs(wcs)

        self.outputQueue.addDataSet(clipboard)
    
    def determineWcs(self, sourceSet, initialWcs):
        """Determine Wcs of an Exposure given a SourceSet and an initial Wcs
        """
        # select sufficiently bright sources
        wcsSourceSet = afwDetection.SourceSet()
        for source in sourceSet:
            if source.getPsfFlux() >= fluxLim:
                wcsSourceSet.append(source)
        
        self.astromSolver.setStarlist(wcsSourceSet)
        self.astromSolver.setNumberStars(len(wcsSourceSet))

        approxPixelScale = 3600.0 * math.sqrt(initialWcs.pixArea(initialWcs.getOriginRaDec()))
        pixelScaleRangeFactor = self._policy.getDouble("pixelScaleRangeFactor")
        self.astromSolver.setMinimumImageScale(approxPixelScale / pixelScaleRangeFactor)
        self.astromSolver.setMaximumImageScale(approxPixelScale * pixelScaleRangeFactor)
        
        # Set parity; once we know it, set it to save time
        # values are: 0: not flipped, 1: flipped, 2: unknown
        self.astromSolver.setParity(2)

        wcs = afwImage.Wcs()
        if self.astromSolver.blindSolve():
            exposure.setWcs(wcs)
        else:
            # \todo: raise an exception once the Wcs determining code is known to be reliable
            self.log.log(Log.ERROR, "Failed to find WCS solution; leaving raw Wcs unchanged")
        return wcs
