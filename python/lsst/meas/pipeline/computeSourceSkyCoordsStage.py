# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#

import math

import lsst.pex.harness.stage as harnessStage

from lsst.pex.logging import Log

import lsst.pex.policy as pexPolicy
import lsst.afw.detection as afwDet
import lsst.afw.coord as afwCoord
import lsst.afw.geom as afwGeom
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
        exposureKey = self.policy.getString("inputKeys.exposure")
        if clipboard.contains(wcsKey):
            wcs = clipboard.get(wcsKey)
        else:
            exposure = clipboard.get(exposureKey)
            wcs = exposure.getWcs()
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

    def raDecWithErrs(self, x, y, xErr, yErr, wcs, pixToSkyAffineTransform=None):
        """Use wcs to transform pixel coordinates x, y and their errors to 
        sky coordinates ra, dec with errors. If the caller does not provide an
        affine approximation to the pixel->sky WCS transform, an approximation
        is automatically computed (and used to propagate errors). For sources
        from exposures far from the poles, a single approximation can be reused
        without introducing much error.

        Note that the affine transform is expected to take inputs in units of
        pixels to outputs in units of degrees. This is an artifact of WCSLIB
        using degrees as its internal angular unit.

        Sky coordinates and their errors are returned in units of radians.
        """
        sky = wcs.pixelToSky(x, y)
        if pixToSkyAffineTransform is None:
            skyp = afwGeom.makePointD(sky.getLongitude(afwCoord.DEGREES),
                                      sky.getLatitude(afwCoord.DEGREES))
            pixToSkyAffineTransform = wcs.linearizeAt(skyp)
        raErr, decErr = self.raDecErrs(xErr, yErr, pixToSkyAffineTransform)
        return (sky.getLongitude(afwCoord.RADIANS),
                sky.getLatitude(afwCoord.RADIANS),
                raErr,
                decErr)

    def raDecErrs(self, xErr, yErr, pixToSkyAffineTransform):
        """Propagates errors in pixel space to errors in ra, dec space
        using an affine approximation to the pixel->sky WCS transform
        (e.g. as returned by lsst.afw.image.Wcs.linearizeAt).

        Note that pixToSkyAffineTransform is expected to take inputs in units
        of pixels to outputs in units of degrees. This is an artifact of WCSLIB
        using degrees as its internal angular unit.

        Errors are returned in units of radians.
        """
        t = pixToSkyAffineTransform
        varRa  = t[0]**2 * xErr**2 + t[2]**2 * yErr**2
        varDec = t[1]**2 * xErr**2 + t[3]**2 * yErr**2
        return (math.radians(math.sqrt(varRa)), math.radians(math.sqrt(varDec)))
         

class ComputeSourceSkyCoordsStage(harnessStage.Stage):
    parallelClass = ComputeSourceSkyCoordsStageParallel
