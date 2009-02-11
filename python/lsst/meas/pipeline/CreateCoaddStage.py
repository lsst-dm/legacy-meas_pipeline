from lsst.pex.harness.Stage import Stage

import lsst.pex.harness.Utils
from lsst.pex.logging import Log, LogRec

import lsst.daf.base as dafBase
from lsst.daf.base import *

class CreateCoaddStage(Stage):
    
    #--------------------------------------------------------------------------
    def process(self):
        """
        Following the example in DMS/coadd/examples:
        For each image in the input image list:
            a. Load each image
            b. Generate the sky function
            c. Generate a psf kernel
        Using the list of (img, skyFunction, psfKernel), make Coadd
        Store the coadd to file.
        Push the the location of the coadd img to clipboard
        """
        #pull out the clipboard from the input queue
        self.activeClipboard = self.inputQueue.getNextDataset()

        root =  Log.getDefaultLog()
        log = Log(root, "lsst.meas.pipeline.CreateCoaddStage.process")

        lr = LogRec(log, Log.INFO)
        lr << " rank " + str(self._rank)
        lr << " stageId " + str(self.stageId) 
        lr << " universeSize " + str(self._universeSize) 
        lr << LogRec.endr

        #push the active clipboard to the output queue
        self.outputQueue.addDataset(self.activeClipboard)
