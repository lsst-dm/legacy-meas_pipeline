from lsst.pex.harness.Stage import Stage

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
        self.log = Log(Log.getDefaultLog(),
                       "lsst.meas.pipeline.PsfDeterminationStage")

    def process(self):
        clipboard = self.inputQueue.getNextDataset()
        try:
            dataList = self._getClipboardData(clipboard)
        except Exception, e:
            self.log.log(Log.FATAL, str(e))
            raise e
        
        for exposure, sourceSet, outKey in dataList:
            psf = self._impl(exposure, sourceSet)
            clipboard.put(outkey, psf)

    def _impl(self, exposure, sourceSet):
        #need to talk to robert for implementation

        #return the psf 
        return None 
   
    def _getClipboardData(self, clipboard):
        dataList = []
        dataPolicyList = self._policy.getPolicyArray("data")
        for dataPolicy in dataPolicyList:
            exposureKey = dataPolicy.getString("exposureKey")
            exposure = clipboard.get(exposureKey)
            if exposure == None:
                raise Exception("No Exposure with key %"%exposureKey) 
            sourceSetKey = dataList.getString("sourceSetKey")
            sourceSet = clipboard.get(sourceSetKey)
            if sourceSet == None:
                raise Exception("No SourceSet with key %"%sourceSetKey)
            outKey = dataPolicy.getString("outputPsfKey")
            dataList.append((exposure, sourceSet, outKey))
        return dataList
