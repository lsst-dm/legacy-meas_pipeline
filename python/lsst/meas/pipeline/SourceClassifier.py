
class SourceClassifier:
    """
    Base class for source classifiers. A SourceClassifier is initialized with a policy, and the position of
    a flag bit it is allowed to set/clear. Once created, classify() is called on the SourceClassifier instance
    some number of times - each time either a single source or a tuple containing two sources is supplied as
    the argument.

    Finally, the finish() method is called on the classifier so that it can log summary statistics and/or
    place SDQA ratings onto a stage clipboard.
    """
    def __init__(self, bit, policy):
        self._bit = bit
        self._policy = policy

    def classify(self, *args):
        """
        Classify one or more sources. To be overriden by subclasses.
        """
        pass

    def finish(self, log, clipboard):
        """
        Lifecycle method to be overriden by subclasses. Called once by SourceClassificationStage
        after the SourceClassifier instance has been used to classify one visits worth of data. Can
        be used to log visit summary statistics or place SDQA ratings onto the stage clipboard.
        """
        pass

    def getPolicy(self):
        """
        Return the policy containing classifier configuration parameters
        """
        return self._policy

    def getMask(self):
        """
        Return a mask for the bit this classifier sets/clears
        """
        return 1 << self._bit

    def getBit(self):
        """
        Return the index of the bit this classifier sets/clears
        """
        return self._bit

    def setBit(self, flag):
        """
        Set the classifier bit in the given integer
        """
        return flag | self.getMask()

    def clearBit(self, flag):
        """
        Clear the classifier bit in the given integer
        """
        return flag ^ (flag & self.getMask())

