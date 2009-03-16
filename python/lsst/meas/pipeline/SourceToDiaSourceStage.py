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

    ClipboardOutput:
    - DiaSourceSet with key outputKey.
    - PersistableDiaSourceVector with key "persistable_"+outputKey
    """
    def __init__(self, stageId = -1, policy = None):
        # call base constructor
        Stage.__init__(self,stageId, policy)
        # initialize a log
        self.log = Log(Log.getDefaultLog(), 
                "lsst.meas.pipeline.SourceToDiaSourceStage")

    def process(self):
        """
        Converting to DiaSource in the worker process
        """
        self.log.log(Log.INFO, "Executing in process")
       
        clipboard = self.inputQueue.getNextDataset()
        keys = self._getPolicyKeys()        
        
        for inKey, outKey in keys:
            sourceSet = clipboard.get(inKey)
            if sourceSet == None:
                raise RuntimeException("SourceSet missing from clipboard")

            diaSourceSet = afwDet.DiaSourceSet()
            for source in sourceSet:
                diaSourceSet.append(afwDet.makeDiaSourceFromSource(source))
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


