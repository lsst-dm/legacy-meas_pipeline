from lsst.pex.harness.Stage import Stage

from lsst.pex.logging import Log

import lsst.pex.policy as policy
import lsst.afw.detection as afwDet
import lsst.afw.image as afwImg
import lsst.pex.exceptions as pexExcept
import lsst.meas.algorithms as measAlg

class SourceToDiaSourceStage(Stage):
    """
    Description:
       Glue stage for transforming clipboard objects from SourceSet 
       to DiaSourceSet

    Policy Dictionaty:
    lsst/meas/pipeline/SourceDetectionStageDictionary.paf

    Clipboard Input:
    - SourceSet with key specified by policy attribute inputKey
    - PersistableSourceVector with key "persistable_"+inputKey 
    - CCD-based WCS with key specified by policy attribute ccdWcsKey

    ClipboardOutput:
    - DiaSourceSet with key outputKey.
    - PersistableDiaSourceVector with key "persistable_"+outputKey
    """
    def process(self):
        """
        Converting to DiaSource in the worker process
        """
        self.log = Log(Log.getDefaultLog(), 
                "lsst.meas.pipeline.SourceToDiaSourceStage")

        self.log.log(Log.INFO, "Executing in process")
       
        clipboard = self.inputQueue.getNextDataset()
        ccdWcsKey = self._policy.get("ccdWcsKey")
        ampBBoxKey = self._policy.getString("ampBBoxKey")
        self.ccdWcs = clipboard.get(ccdWcsKey)
        self.ampBBox = clipboard.get(ampBBoxKey)
        keys = self._getPolicyKeys()

        for inKey, outKey in keys:
            sourceSet = clipboard.get(inKey)
            if sourceSet == None:
                raise RuntimeException("SourceSet missing from clipboard")

            diaSourceSet = afwDet.DiaSourceSet()
            for source in sourceSet:
                diaSource = afwDet.makeDiaSourceFromSource(source)

                (ra, dec, raErr, decErr) = self.raDecWithErrs(
                        diaSource.getXFlux(), diaSource.getYFlux(),
                        diaSource.getXFluxErr(), diaSource.getYFluxErr())
                diaSource.setRaFlux(ra); diaSource.setDecFlux(dec)
                diaSource.setRaFluxErr(raErr); diaSource.setDecFluxErr(decErr)

                (ra, dec, raErr, decErr) = self.raDecWithErrs(
                        diaSource.getXAstrom(), diaSource.getYAstrom(),
                        diaSource.getXAstromErr(), diaSource.getYAstromErr())
                diaSource.setRaAstrom(ra); diaSource.setDecAstrom(dec)
                diaSource.setRaAstromErr(raErr); diaSource.setDecAstromErr(decErr)

                # No errors for XPeak, YPeak
                raDec = self.ccdWcs.xyToRaDec(
                        diaSource.getXPeak(), diaSource.getYPeak())
                diaSource.setRaPeak(raDec.getX())
                diaSource.setDecPeak(raDec.getY())

                # Simple RA/decl == Astrom versions
                diaSource.setRa(diaSource.getRaAstrom())
                diaSource.setRaErrForDetection(diaSource.getRaAstromErr())
                diaSource.setDec(diaSource.getDecAstrom())
                diaSource.setDecErrForDetection(diaSource.getDecAstromErr())

                diaSourceSet.append(diaSource)

            persistableSet = afwDet.PersistableDiaSourceVector(diaSourceSet)

            clipboard.put(outKey, diaSourceSet)
            clipboard.put("persistable_" + outKey, persistableSet)
        
        self.outputQueue.addDataset(clipboard)
    
    def _getPolicyKeys(self):
        """
        parse policy object into more useful form
        """
        keys = []
        for item in self._policy.getPolicyArray("data"):
            inputKey = item.getString("inputKey")
            outputKey = item.getString("outputKey")
            keys.append((inputKey, outputKey))

        return keys

    def raDecWithErrs(self, x, y, xErr, yErr):
        # ccdWcs is determined from a CCDs worth of WcsSources (by each slice in the CCD),
        # but is shifted to amp relative coordinates. XY coords are CCD relative, so transform
        # to the amp coordinate system before using the WCS.
        ampX = x - self.ampBBox.getX0()
        ampY = y - self.ampBBox.getY0()
        raDec = self.ccdWcs.xyToRaDec(ampX, ampY)
        ra = raDec.getX(); dec = raDec.getY()
        raDecWithErr = self.ccdWcs.xyToRaDec(ampX + xErr, ampY + yErr)
        raErr = raDecWithErr.getX() - ra
        decErr = raDecWithErr.getY() - dec
        return (ra, dec, raErr, decErr)
