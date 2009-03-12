from lsst.pex.harness.Stage import Stage

# import lsst.pex.harness.Utils
from lsst.pex.logging import Log

import lsst.daf.base as dafBase
import lsst.afw.detection as afwDet
import lsst.afw.image as afwImgage
import lsst.meas.astrom.net as astromNet

class WcsDeterminationStage(Stage):
    """Refine a WCS in an Exposure based on a list of sources

    See detection/pipeline/WcsDeterminationStageDictionary.paf
    
    DO WE NEED A CUT ON FLUX LIMIT? OR IS DETECTION DOING THAT FOR US?

    Clipboard Input:
    - An Exposure whose name is given in the policy file
    - A SourceSet whose name is given in the policy file

    ClipboardOutput:
    - The same Exposure, with refined Wcs
    
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
        self.log = Log(Log.getDefaultLog(), 
                "lsst.meas.pipeline.WcsDeterminationStage")
        self.astromSolver = None
    
    def process(self):
        """Determine Wcs"""
        self.log.log(Log.INFO, "Wcs Determination Stage")
        exposureNameList = self._policy.getStringArray("exposureNames")
        sourceSetNameList = self._policy.getStringArray("sourceSetNames")
        self.fluxLimit = self._policy.getDouble("fluxLimit")
        if len(exposureNameList) != len(sourceSetNameList):
            raise RuntimeError("Numer of exposureNames = %s != number of sourceSetNames %s" %
                (len(exposureNames), len(sourceSetNames)))
        
        # read in astrometry.net indices, if not already done
        # todo: only read in required portions or mmap the files
        if self.astromSolver == None:
            self.astromSolver = astromNet.GlobalAstrometrySolution()
            #Read in the indices (i.e the files containing the positions of known asterisms
            #and add them to the astromSolver object
            astrometryIndicesGlob = self._policy.getString("astrometryIndicesGlob")
            self.log.log(Log.INFO, "Reading astrometry index files %s" % (astrometryIndicesGlob,))
            indexPathList = glob.glob(astrometryIndicesGlob)
            for indexPath in indexPathList:
                self.astromSolver.addIndexFile(indexPath)

        # set parameters
        allowDistortion = self._policy.getBoolean("allowDistortion")
        matchThreshold = self._policy.getDouble("matchThreshold")
        self.astromSolver.allowDistortion(allowDistortion)
        self.astromSolver.setMatchThreshold(matchThreshold)

        for exposureName, sourceSetName in zip(exposureNameList, sourceSetNameList):
            self.log.log(Log.INFO, "Determine Wcs of exposure %s using sourceSet %s" % (exposureName, sourceSetName))
            exposure = clipboard.get(exposureName)
            sourceSet = clipboard.get(sourceSetName)
            self.processOneExposure(exposure, sourceSet)

    def processOneExposure(self, exposure, sourceSet):
        """Determine Wcs of an Exposure given a SourceSet
        """
        # select sufficiently bright sources
        wcsSourceSet = afwDetection.SourceSet()
        for source in sourceSet:
            if source.getPsfFlux() >= fluxLim:
                wcsSourceSet.append(source)
        
        self.astromSolver.setStarlist(wcsSourceSet)
        self.astromSolver.setNumberStars(len(wcsSourceSet))

        wcs = exposure.getWcs()
        approxPixelScale = 3600.0 * math.sqrt(wcs.pixArea(wcs.getOriginRaDec()))
        pixelScaleRangeFactor = self._policy.getDouble("pixelScaleRangeFactor")
        self.astromSolver.setMinimumImageScale(approxPixelScale / pixelScaleRangeFactor)
        self.astromSolver.setMaximumImageScale(approxPixelScale * pixelScaleRangeFactor)

        if self.astromSolver.blindSolve():
            exposure.setWcs(wcs)
        else:
            # \todo: raise an exception once the Wcs determining code is known to be reliable
            self.log.log(Log.ERROR, "Failed to find WCS solution; leaving raw Wcs unchanged")
