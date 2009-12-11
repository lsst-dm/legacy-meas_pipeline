import lsst.pex.harness.stage as harnessStage

from lsst.pex.logging import Log

import lsst.pex.policy as pexPolicy
import lsst.afw.detection as afwDet
import lsst.afw.image as afwImg
import lsst.pex.exceptions as pexExcept
import lsst.meas.algorithms as measAlg

class SourceToDiaSourceStageParallel(harnessStage.ParallelProcessing):
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
    def setup(self):
        self.log = Log(self.log, "SourceToDiaSourceStage - parallel")

        policyFile = pexPolicy.DefaultPolicyFile("meas_pipeline", 
            "SourceToDiaSourceStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath(), True)

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy.getDictionary())


    def process(self, clipboard):
        """
        Converting to DiaSource in the worker process
        """
        self.log.log(Log.INFO, "Executing in process")
       
        self.ccdWcs = clipboard.get(self.policy.get("ccdWcsKey"))
        self.ampBBox = clipboard.get(self.policy.getString("ampBBoxKey"))

        dataPolicyList = self.policy.getPolicyArray("data")
        for dataPolicy in dataPolicyList:
            inputKey = dataPolicy.getString("inputKey")
            outputKey = dataPolicy.getString("outputKey")

            sourceSet = clipboard.get(inputKey)
            if sourceSet is None:
                self.log.log(Log.FATAL, "No SourceSet with key " + inputKey)
                continue

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

            clipboard.put(outputKey, diaSourceSet)
            clipboard.put("persistable_" + outputKey, persistableSet)
        
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

class SourceToDiaSourceStage(harnessStage.Stage):
    parallelClass = SourceToDiaSourceStageParallel
