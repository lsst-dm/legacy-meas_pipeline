import math
from math import *

from lsst.pex.logging import Log, Rec
from lsst.pex.harness.Stage import Stage
import lsst.pex.policy as policy
import lsst.afw.image as afwImg
import lsst.afw.detection as afwDet
import lsst.afw.math as afwMath
import lsst.meas.algorithms as algorithms
import lsst.meas.algorithms.Psf as Psf
import lsst.sdqa as sdqa

class PsfDeterminationStage(Stage):
    """
    Given an exposure and a set of sources measured on that exposure,
    determine a PSF for that exposure.

    This stage works on lists of (exposure, sourceSet) pairs.

    Their location on the clipboard is specified via policy.
    see lsst/meas/pipeline/pipeline/PsfDeterminationStageDictionary.paf
    for details on configuring valid stage policies
    """
    def __init__(self, stageId = -1, policy = None):
        Stage.__init__(self, stageId, policy)
        self._badFlags = algorithms.Flags.EDGE | \
                         algorithms.Flags.INTERP_CENTER | \
                         algorithms.Flags.SATUR_CENTER | \
                         algorithms.Flags.PEAKCENTER
        self.log = Log(Log.getDefaultLog(),
                       "lsst.meas.pipeline.PsfDeterminationStage")

    def process(self):
        self._validatePolicy()
        clipboard = self.inputQueue.getNextDataset()
        try:
            dataList = self._getClipboardData(clipboard)
        except Exception, e:
            self.log.log(Log.FATAL, str(e))
            raise 
        
        sdqaRatings = sdqa.SdqaRatingSet()

        for exposure, sourceList, outKey, outputCellSetKey in dataList:
            psf, cellSet = Psf.getPsf(exposure, sourceList, self._policy, sdqaRatings)
            clipboard.put(outKey, psf)
            clipboard.put(outputCellSetKey, cellSet)

        sdqaKey = self._policy.getString("PsfDeterminationSdqaKey")
        clipboard.put(sdqaKey, sdqa.PersistableSdqaRatingVector(sdqaRatings))

        self.outputQueue.addDataset(clipboard)

    def _getClipboardData(self, clipboard):
        dataList = []
        dataPolicyList = self._policy.getPolicyArray("data")
        for dataPolicy in dataPolicyList:
            exposureKey = dataPolicy.getString("exposureKey")
            exposure = clipboard.get(exposureKey)
            if exposure == None:
                raise Exception("No Exposure with key %s" %exposureKey) 
            sourceSetKey = dataPolicy.getString("sourceSetKey")
            sourceSet = clipboard.get(sourceSetKey)
            if sourceSet == None:
                raise Exception("No SourceSet with key %"%sourceSetKey)
            psfOutKey = dataPolicy.getString("outputPsfKey")
            outputCellSetKey = dataPolicy.getString("outputCellSetKey")
            dataList.append((exposure, sourceSet, psfOutKey, outputCellSetKey))
            
        return dataList

    def _validatePolicy(self):
        # N.b. these aren't used in self, but the validation can't hurt --- until we have a dictionary
        
        self._fluxLim = self._policy.get("fluxLim")
        
        self._nEigenComponents = self._policy.getInt("nEigenComponents")
        self._spatialOrder  = self._policy.getInt("spatialOrder")
        self._nStarPerCell = self._policy.getInt("nStarPerCell")
        self._kernelSize = self._policy.getInt("kernelSize")
        self._nStarPerCellSpatialFit = \
                self._policy.getInt("nStarPerCellSpatialFit")
        self._tolerance = self._policy.getDouble("tolerance")
        self._reducedChi2ForPsfCandidates = \
                self._policy.getDouble("reducedChi2ForPsfCandidates")
        self._nIterForPsf = self._policy.getInt("nIterForPsf")
        self._sizeCellX = self._policy.getInt("sizeCellX")
        self._sizeCellY = self._policy.getInt("sizeCellY")
