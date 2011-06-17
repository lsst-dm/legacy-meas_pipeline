# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#
import lsst.utils

def version():
    """Return current version. If a different version is setup, return that too"""

    HeadURL = r"$HeadURL$"
    return lsst.utils.version(HeadURL, "meas.pipeline")

from sourceMeasurementStage import *
from sourceMeasurementPsfFluxStage import *
from sourceDetectionStage import *
from psfDeterminationStage import *
from apertureCorrectionStage import *
from apertureCorrectionApplyStage import *
from wcsDeterminationStage import *
from sourceClassificationStage import *
from sourceToDiaSourceStage import *
#from transformDetectionStage import *
#from multifitStage import *
from multifitFlagIngestStage import *
from forcedPhotometryStage import *
from backgroundEstimationStage import *
from wcsVerificationStage import *
from photoCalStage import *
from computeSourceSkyCoordsStage import *
try:
    from singleFrameMultifitStage import *
except ImportError:
    pass                                # it's setupOptional
