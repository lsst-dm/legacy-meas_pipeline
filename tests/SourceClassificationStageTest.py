import os, os.path
import unittest

import lsst.pex.policy as policy
import lsst.pex.harness as harness
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

        clipboard = harness.Clipboard()
        clipboard.put("sourceSet0", set0)
        clipboard.put("sourceSet1", set1)
        inq = harness.Queue()
        outq = harness.Queue()
        stage.setUniverseSize(1)
        stage.setRank(0)
        stage.initialize(outq, inq)
        inq.addDataset(clipboard)

        stage.process()


if __name__ == "__main__":
    unittest.main()

