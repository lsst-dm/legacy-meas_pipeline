import itertools
import pdb
import sys

from SourceClassifier import SourceClassifier

from lsst.pex.harness.Stage import Stage
from lsst.pex.logging import Log, LogRec, endr
from lsst.pex.policy import Policy


class SourceClassificationStage(Stage):
    """
    This stage implements source classification on pairs of sources.
    Two lists of sources, each corresponding to measurements from one
    of two per-visit exposures are expected to be passed in on the clipboard.
    These lists are expected to have identical size and order, such that the
    sources at position i in both lists are the pair of measurements
    from a single detection on the addition of a visits two difference images.
    The clipboard key names of these two lists is configurable via policy.

    The stage policy is additionally expected to contain a "Classifiers" key
    that describes up to 64 classifiers (which set source flag bits). Each of
    these must contain the following child keys:
    - "bits"         array of flag bits computed by the classifier (0-63)
    - "pythonClass"  the name of the python class for the classifier
    - "arguments"    "first":  process the first source list only
                     "second": process the second source list only
                     "both":   process both source lists
                     "pairs":  process pairs of sources from both lists
    - "parameters"   contains classifier specific configuration parameters 

    Note that classifiers are run in the order in which they appear in the
    stage policy file.

    See pipeline/SourceClassificationStageDictionary.paf for the details
    (including classifier specific parameters).

    Clipboard Input:
    - First difference source list (name determined by "sourceList1ClipboardKey" policy key)
    - Second difference source list (name determined by "sourceList2ClipboardKey" policy key)

    Clipboard Output:
    - the input clipboard is passed to the next stage with no structural
      modifications. Only individual sources are modified by this stage.
    """

    class _Classifier(object):
        """
        Helper class for importing and running source classifiers.
        """
        def __init__(self, policy, log):
            bits = policy.getIntArray("bits")
            pythonClass = policy.getString("pythonClass")
            components = pythonClass.split('.')
            className = components.pop()
            if len(components) == 0:
                raise RuntimeError("SourceClassifier package must be fully specified")
            else:
                try:
                    moduleName = '.'.join(components)
                    __import__(moduleName)
                    self._pythonClass = sys.modules[moduleName].__dict__[className]
                except Exception, e:
                    raise RuntimeError("Failed to instantiate class for SourceClassifier %s" % pythonClass)
            self._scope = policy.getString("scope")
            subpolicy = policy.getPolicy("parameter") if policy.exists("parameter") else Policy()
            if not issubclass(self._pythonClass, SourceClassifier):
                raise RuntimeError("%s is not a subclass of SourceClassifier - check stage policy" % pythonClass)
            # Create classifier, specifying a flag bit position
            self._classifier = self._pythonClass(bits, subpolicy)
            rec = LogRec(log, Log.INFO)
            rec << "Registered SourceClassifier" << { "pythonClass": pythonClass }
            for bit in bits:
                rec << { "bit": bit }
            rec << endr

        def invoke(self, sourceList1, sourceList2):
            if self._scope == "first":
                for source in sourceList1:
                    self._classifier.classify(source)
            elif self._scope == "second":
                for source in sourceList2:
                    self._classifier.classify(source)
            elif self._scope == "both":
                for source in itertools.chain(sourceList1, sourceList2):
                    self._classifier.classify(source)
            elif self._scope == "pairs":
                if sourceList1.size() != sourceList2.size():
                    raise RuntimeError("Source lists passed to classifier must have identical length")
                for s0, s1 in itertools.izip(sourceList1, sourceList2):
                    self._classifier.classify(s0, s1)
            else:
                raise RuntimeError("Unsupported source classifier argument type - check stage policy")

        def finish(self, log):
            self._classifier.finish(log)


    def __init__(self, stageId, policy):
        Stage.__init__(self, stageId, policy)
        self._log = Log(Log.getDefaultLog(), "lsst.meas.pipeline.SourceClassificationStage")
        self._list1Key = policy.getString("sourceList0ClipboardKey")
        self._list2Key = policy.getString("sourceList1ClipboardKey")
        self._policies = policy.getPolicyArray("classifier") if policy.exists("classifier") else []


    def process(self):
        """
        Classify sources in the worker process
        """
        clipboard = self.inputQueue.getNextDataset()
        sourceList1 = clipboard.get(self._list1Key)
        sourceList2 = clipboard.get(self._list2Key)
        classifiers = [ SourceClassificationStage._Classifier(p, self._log) for p in self._policies ]

        for c in classifiers:
            c.invoke(sourceList1, sourceList2)
        for c in classifiers:
            c.finish(self._log)
        self.outputQueue.addDataset(clipboard)

