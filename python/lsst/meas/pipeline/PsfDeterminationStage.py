import math
from math import *

from lsst.pex.logging import Log, Rec
from lsst.pex.harness.Stage import Stage
import lsst.pex.policy as policy
import lsst.afw.image as afwImg
import lsst.afw.detection as afwDet
import lsst.afw.math as afwMath
import lsst.meas.algorithms as algorithms

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
        
        for exposure, sourceList, outKey, outputCellSetKey in dataList:
            psf, cellSet = self._impl(exposure, sourceList)
            clipboard.put(outKey, psf)
            clipboard.put(outputCellSetKey, cellSet)

        self._outputQueue.addDataset(clipboard)

    def _impl(self, exposure, sourceList):
        #
        # Create an Image of Ixx v. Iyy, i.e. a 2-D histogram
        #
        psfHist = algorithms.PsfShapeHistogram()

        for source in sourceList:
            if self._goodPsfCandidate(source):
                psfHist.insert(source)
        
        clump = psfHist.getClump()
        psfClumpX, psfClumpY, psfClumpIxx, psfClumpIxy, psfClumpIyy = clump
        
        #
        # Go through and find all the PSF-like objects
        #
        mi = exposure.getMaskedImage()
        #
        # We'll split the image into a number of cells, 
        # each of which contributes only one PSF candidate star
        #
        sizePsfCellX = self._sizeCellX
        sizePsfCellY = self._sizeCellY
        
        bbox = afwImg.BBox(mi.getXY0(), mi.getWidth(), mi.getHeight())
        psfCellSet = afwMath.SpatialCellSet(bbox, 
                                            self._sizeCellX, 
                                            self._sizeCellY)
        
        psfStars = []
        
        det = psfClumpIxx*psfClumpIyy - psfClumpIxy*psfClumpIxy
        try:
            a, b, c = psfClumpIyy/det, -psfClumpIxy/det, psfClumpIxx/det
        except ZeroDivisionError:
            a, b, c = 1e4, 0, 1e4
        
        for source in sourceList:
            Ixx, Ixy, Iyy = source.getIxx(), source.getIxy(), source.getIyy()
            dx, dy = (Ixx - psfClumpX), (Iyy - psfClumpY)
            
            # A test for > would be confused by NaN's
            if math.sqrt(a*dx*dx + 2*b*dx*dy + c*dy*dy) < 2: 
                if not self._goodPsfCandidate(source):
                    continue
            try:
                psfCandidate = algorithms.makePsfCandidate(source, mi)
                psfCellSet.insertCandidate(psfCandidate)
            except Exception, e:
                continue
            
            psfStars += [source]
        
        #
        # setWidth/setHeight are class static, 
        # but we'd need to know that the class was <float> to use that info; 
        # e.g.
        #   afwMath.SpatialCellImageCandidateF_setWidth(21)
        #
        psfCandidate = algorithms.makePsfCandidate(source, mi)
        psfCandidate.setWidth(21)
        psfCandidate.setHeight(21)
        del psfCandidate
        
        #
        # Do a PCA decomposition of those PSF candidates
        #
        for iter in range(self._nIterForPsf):
            for cell in psfCellSet.getCellList():
                cell.setIgnoreBad(True)
            
            pair = algorithms.createKernelFromPsfCandidates(psfCellSet, 
                                                self._nEigenComponents, 
                                                self._spatialOrder,
                                                self._kernelSize, 
                                                self._nStarPerCell)
            kernel, eigenValues = pair[0], pair[1]; 
            del pair
            
            pair = algorithms.fitSpatialKernelFromPsfCandidates(kernel, 
                                                psfCellSet, 
                                                self._nStarPerCellSpatialFit,
                                                self._tolerance)
            status, chi2 = pair[0], pair[1]; 
            del pair
            
            psf = algorithms.createPSF("PCA", kernel)
            psfCandidate = algorithms.makePsfCandidate(source, mi)
            
            # number of degrees of freedom/star for chi2
            nu = psfCandidate.getWidth()*psfCandidate.getHeight() - 1 
            del psfCandidate
            
            for cell in psfCellSet.getCellList():
                cell.setIgnoreBad(True)
                for cand in cell:
                    cand = algorithms.cast_PsfCandidateF(cand)
                    rchi2 = cand.getChi2()/nu
                    
                    if rchi2 > self._reducedChi2ForPsfCandidates:
                        cand.setStatus(afwMath.SpatialCellCandidate.BAD)
                
                cell.setIgnoreBad(True)
        
        return (psf, psfCellSet) 
  
    def _goodPsfCandidate(self, source):
        """Should this object be included in the Ixx v. Iyy image?"""
        if source.getFlagForDetection() & self._badFlags or \
           source.getPsfFlux() < self._fluxLim: 
            # ignore flagged or faint objects
            return False

        return True

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
        if self._policy.exists("fluxLim"):
            self._fluxLim = self._policy.get("fluxLim")
        else:
            self._fluxLim = 1000
        
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
