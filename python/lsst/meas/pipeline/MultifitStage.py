from lsst.pex.harness.Stage import Stage

import lsst.pex.harness.Utils
from lsst.pex.logging import Log, LogRec

import lsst.daf.base as dafBase
from lsst.daf.base import *

class MultifitStage(Stage):
    
    def process(self):
        """
        Query DB for list of Detections
        Select a subset of the master list based on _universeSize and _rank
        For each detection in subset of detection list
            generate initial model of the detection
            For each img in input image list:
                                
        """
        #pull out the clipboard from the input queue
        self.activeClipboard = self.inputQueue.getNextDataset()

        root =  Log.getDefaultLog()
        log = Log(root, "lsst.meas.pipeline.MultifitStage.process")

        lr = LogRec(log, Log.INFO)
        lr << " rank " + str(self._rank)
        lr << " stageId " + str(self.stageId) 
        lr << " universeSize " + str(self._universeSize) 
        lr << LogRec.endr

        #push the active clipboard to the output queue
        self.outputQueue.addDataset(self.activeClipboard)

