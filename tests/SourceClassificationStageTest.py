import os, os.path
import pdb
import unittest

from lsst.pex.harness.Clipboard import  Clipboard
from lsst.pex.harness.Queue import Queue
import lsst.pex.policy as policy
import lsst.afw.detection as detection
import lsst.meas.pipeline as pipeline


class SourceClassificationStageTestCase(unittest.TestCase):
    """A test case for the SourceClassificationStage"""

    def setUp(self):
       self._policy = policy.Policy(os.path.join(os.environ["MEAS_PIPELINE_DIR"],
                                                 "tests", "SourceClassificationStageTest.paf"))
       self._stage = pipeline.SourceClassificationStage(0, self._policy)

    def tearDown(self):
        del self._stage
        del self._policy

    def testStage(self):
        set0 = detection.DiaSourceSet()
        set1 = detection.DiaSourceSet()
        set0.append(detection.DiaSource())
        set0.append(detection.DiaSource())
        set0.append(detection.DiaSource())
        set1.append(detection.DiaSource())
        set1.append(detection.DiaSource())
        set1.append(detection.DiaSource())

        clipboard = Clipboard()
        clipboard.put("sourceSet0", set0)
        clipboard.put("sourceSet1", set1)
        inq = Queue()
        outq = Queue()
        self._stage.setUniverseSize(1)
        self._stage.setRank(0)
        self._stage.initialize(outq, inq)
        inq.addDataset(clipboard)

        self._stage.process()


if __name__ == "__main__":
    unittest.main()

