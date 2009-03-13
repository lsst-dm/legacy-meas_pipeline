"""
Classes which classify sources based on their attributes.
"""
import math

from SourceClassifier import SourceClassifier
from lsst.pex.logging import Log, LogRec, endr


class PresentInExposureClassifier(SourceClassifier):
    """
    Check whether the absolute value of the PSF flux of a source is above some threshold.

    Configuration parameters:
        "psfFluxThreshold": if the absolute value of the PSF flux of a source is below this
                            threshold, the source is considered to be missing (this happens
                            when source detection and measurement happen on different 
                            exposures).
    """
    def __init__(self, bit, policy):
        SourceClassifier.__init__(self, bit, policy)
        self._psfFluxThreshold = policy.getDouble("psfFluxThreshold")
        self._numPresent = 0
        self._numMissing = 0
        assert self._psfFluxThreshold > 0

    def classify(self, *args):
        source = args[0]
        flag = source.getFlagClassification()
        if abs(source.getPsfFlux()) > self._psfFluxThreshold:
            source.setFlagClassification(self.setBit(flag))
            self._numPresent += 1
        else:
            source.setFlagClassification(self.clearBit(flag))
            self._numMissing += 1

    def finish(self, log=None, clipboard=None):
        if log:
            LogRec(log, Log.INFO) << "PresentInExposureClassifer visit statistics" << \
                { "numPresent": self._numPresent, "numMissing": self._numMissing } << endr


class PresentInBothExposuresClassifier(SourceClassifier):
    """
    Check whether the absolute values of the PSF fluxes of two sources are both above a threshold.

    Configuration parameters:
        "psfFluxThreshold": if the absolute value of the PSF flux of a source is below this
                            threshold, the source is considered to be missing from an exposure
                            (this happens when source detection and measurement happen on
                            different exposures).
    """
    def __init__(self, bit, policy):
        SourceClassifier.__init__(self, bit, policy)
        self._numBothPresent = 0
        self._numOnePresent = 0
        self._psfFluxThreshold = policy.getDouble("psfFluxThreshold")
        assert self._psfFluxThreshold > 0

    def classify(self, *args):
        flag0 = args[0].getFlagClassification()
        flag1 = args[1].getFlagClassification()
        if (abs(args[0].getPsfFlux()) > self._psfFluxThreshold and
            abs(args[1].getPsfFlux()) > self._psfFluxThreshold):
            args[0].setFlagClassification(self.setBit(flag0))
            args[1].setFlagClassification(self.setBit(flag1))
            self._numBothPresent += 1
        else:
            args[0].setFlagClassification(self.clearBit(flag0))
            args[1].setFlagClassification(self.clearBit(flag1))
            self._numOnePresent += 1

    def finish(self, log=None, clipboard=None):
        if log:
            LogRec(log, Log.INFO) << "PresentInBothExposuresClassifier visit statistics" << \
                { "numBothPresent": self._numBothPresent, "numOnePresent": self._numOnePresent } << endr


class ShapeDiffersInExposuresClassifier(SourceClassifier):
    """
    Check whether the shapes of two difference sources differ significantly.
    Probably bogus and needs proof reading.

    Configuration parameters:
        "shapeNormDiffThreshold": if the absolute value of the difference of the shape
                                  parameter norms for two source is greater than this
                                  threshold, then the sources are considered to have
                                  different shape.
    """
    def __init__(self, bit, policy):
        SourceClassifier.__init__(self, bit, policy)
        self._numDifferentShape = 0
        self._numSimilarShape = 0
        self._shapeNormDiffThreshold = policy.getDouble("shapeNormDiffThreshold")
        assert self._shapeNormDiffThreshold > 0

    def _shapeNorm(self, ixx, iyy, ixy):
        """
        Computes norm of shape parameters
        """
        if ixx + iyy != 0.0:
            e1 = (ixx - iyy)/(ixx + iyy)
            e2 = 2*ixy/(ixx + iyy)
            return math.sqrt(e1*e1 + e2*e2)
        else: return 0.0

    def classify(self, *args):
        flag0 = args[0].getFlagClassification()
        flag1 = args[1].getFlagClassification()
        sn0 = self._shapeNorm(args[0].getIxx(), args[0].getIyy(), args[0].getIxy())
        sn1 = self._shapeNorm(args[1].getIxx(), args[1].getIyy(), args[1].getIxy())
        if abs(sn0 - sn1) > self._shapeNormDiffThreshold:
            args[0].setFlagClassification(self.setBit(flag0))
            args[1].setFlagClassification(self.setBit(flag1))
            self._numDifferentShape += 1
        else:
            args[0].setFlagClassification(self.clearBit(flag0))
            args[1].setFlagClassification(self.clearBit(flag1))
            self._numSimilarShape += 1

    def finish(self, log=None, clipboard=None):
        if log:
            LogRec(log, Log.INFO) << "ShapeDiffersInExposuresClassifier visit statistics" << \
                { "numDifferentShape": self._numDifferentShape, "numSimilarShape": self._numSimilarShape } << endr


class PositiveFluxExcursionClassifier(SourceClassifier):
    """
    Checks whether the flux excursion of a source is positive (for difference
    sources it can be negative).

    Configuration parameters:
        "psfFluxThreshold": if the PSF flux of a source is above this threshold,
                            the source is considered to be a positive excursion
    """
    def __init__(self, bit, policy):
        SourceClassifier.__init__(self, bit, policy)
        self._numPositive = 0
        self._numMissingOrNegative = 0
        self._psfFluxThreshold = policy.getDouble("psfFluxThreshold")
        assert self._psfFluxThreshold > 0

    def classify(self, source):
         flag = source.getFlagClassification()
         if source.getPsfFlux() > self._psfFluxThreshold:
             source.setFlagClassification(self.setBit(flag))
             self._numPositive += 1
         else:
             source.setFlagClassification(self.clearBit(flag))
             self._numMissingOrNegative += 1

    def finish(self, log=None, clipboard=None):
        if log:
            LogRec(log, Log.INFO) << "PositiveFluxExcursion visit statistics" << \
                { "numPositive": self._numPositive, "numMissingOrNegative": self._numMissingOrNegative } << endr


class EllipticalAfterPSFDeconvolveClassifier(SourceClassifier):
    """
    Not available for DC3a. RHL says it is possible to estimate this from global
    PSF info and second moments, but it's not totally trivial, he's quite busy,
    and we really want to use local PSF information in the long run.
    """
    pass

