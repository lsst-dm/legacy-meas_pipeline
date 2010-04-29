import lsst.pex.harness.stage as harnessStage

from lsst.pex.logging import Log

import lsst.pex.policy as pexPolicy
import lsst.afw.detection as afwDet
import lsst.afw.coord as afwCoord
import lsst.pex.exceptions as pexExcept


class ComputeSourceSkyCoordsStageParallel(harnessStage.ParallelProcessing):
    """
    Description:
       Stage that converts pixel coordinates (X,Y) to sky coordinates
       (ra,dec) (in radians) for all sources in a SourceSet. Note
       that sources are updated in place and that the clipboard structure
       is not changed in any way.

    Policy Dictionary:
        lsst/meas/pipeline/SourceXYToRaDecStageDictionary.paf
    """
    def setup(self):
        self.log = Log(self.log, "ComputeSourceSkyCoordsStage - parallel")
        policyFile = pexPolicy.DefaultPolicyFile(
            "meas_pipeline", "ComputeSourceSkyCoordsStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(
            policyFile, policyFile.getRepositoryPath(), True)
        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy.getDictionary())

    def process(self, clipboard):
        wcsKey = self.policy.getString("inputKeys.wcs")
        sourcesKey = self.policy.getString("inputKeys.sources")
        wcs = clipboard.get(wcsKey)
        sourceSet = clipboard.get(sourcesKey)
        if sourceSet is None:
            self.log.log(Log.WARN, "No SourceSet with key " + sourcesKey)
            return
        for s in sourceSet:
            (ra, dec, raErr, decErr) = self.raDecWithErrs(
                s.getXFlux(), s.getYFlux(),
                s.getXFluxErr(), s.getYFluxErr(), wcs)
            s.setRaFlux(ra);
            s.setDecFlux(dec)
            s.setRaFluxErr(raErr);
            s.setDecFluxErr(decErr)

            (ra, dec, raErr, decErr) = self.raDecWithErrs(
                s.getXAstrom(), s.getYAstrom(),
                s.getXAstromErr(), s.getYAstromErr(), wcs)
            s.setRaAstrom(ra);
            s.setDecAstrom(dec)
            s.setRaAstromErr(raErr);
            s.setDecAstromErr(decErr)

            # No errors for XPeak, YPeak
            coords = wcs.pixelToSky(s.getXPeak(), s.getYPeak())
            s.setRaPeak(coords.getLongitude(afwCoord.RADIANS))
            s.setDecPeak(coords.getLatitude(afwCoord.RADIANS))

            # Simple RA/decl == Astrom versions
            s.setRa(s.getRaAstrom())
            s.setRaErrForDetection(s.getRaAstromErr())
            s.setDec(s.getDecAstrom())
            s.setDecErrForDetection(s.getDecAstromErr())

    def raDecWithErrs(self, x, y, xErr, yErr, wcs):
        """Use wcs to transform pixel coordinates x,y to sky coordinates ra,dec.
        """
        raDec = wcs.pixelToSky(x, y)
        ra = raDec.getLongitude(afwCoord.RADIANS)
        dec = raDec.getLatitude(afwCoord.RADIANS)
        raDecWithErr = wcs.pixelToSky(x + xErr, y + yErr)
        raErr = raDecWithErr.getLongitude(afwCoord.RADIANS) - ra
        decErr = raDecWithErr.getLatitude(afwCoord.RADIANS) - dec
        return (ra, dec, raErr, decErr)

class ComputeSourceSkyCoordsStage(harnessStage.Stage):
    parallelClass = ComputeSourceSkyCoordsStageParallel
