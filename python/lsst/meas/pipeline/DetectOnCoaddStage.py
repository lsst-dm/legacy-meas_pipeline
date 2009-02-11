from lsst.pex.harness.Stage import Stage

import lsst.pex.harness.Utils
from lsst.pex.logging import Log, LogRec

import lsst.daf.base as dafBase
from lsst.daf.base import *

class DetectOnCoaddStage(Stage):
    #--------------------------------------------------------------------------
    def process(self):
        """
        1. Load CoAdd
        2. Run detect on Coadd
        3. Persist detectionList to DB
        """
        #pull out the clipboard from the input queue
        self.activeClipboard = self.inputQueue.getNextDataset()
        
        root =  Log.getDefaultLog()
        log = Log(root, "lsst.meas.pipeline.DetectOnCoaddStage.process")

        lr = LogRec(log, Log.INFO)
        lr << " rank " + str(self._rank)
        lr << " stageId " + str(self.stageId) 
        lr << " universeSize " + str(self._universeSize) 
        lr << LogRec.endr

        #push the active clipboard to the output queue
        self.outputQueue.addDataset(self.activeClipboard)
