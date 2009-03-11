#!/usr/bin/env python

import lsst.pex.policy as policy
import lsst.pex.exceptions as pexExcept
from lsst.pex.logging import Log, Rec
import lsst.afw.image as afwImg
import lsst.afw.math as afwMath
from SourceDetectionStage import SourcedetectionStage

class AddAndDetectStage(SourceDetectionStage):
    def __addAndDetect__(self):
        exposure0 = clipboard.get(self.__exposureKey0__)
        exposure1 = clipboard.get(self.__exposureKey1__)
        
        if exposure1 == None or exposure2 == None:
            self.log.log(FATAL "Input exposures missing - ignoring dataset")
            return
        
        addedExposure = self.__addExposures(exposure1, exposure2)
        dsPositive, dsNegative = self.__detectSourcesImpl__(addedExposure, psf)
        
        if dsPositive != None:
            clipboard.put("PositiveFootprintSet", dsPositive)
        if dsNegative != None:
            clipboard.put("NegativeFootprintSet", dsNegative)
        self.outputQueue.addDataset(clipboard)

    def __addExposures__(self, exposure1, exposure2):
        image1 = exposure1.getMaskedImage()
        image2 = exposure2.getMaskedImage()

        addedImage = image1.Factory(image1.getDimensions(), 
                                    image1.getMask().getMaskPlaneDict())
        origin = afwImg.PointI(image1.getX0(), image1.getY0())
        addedImage.setXYO(origin)
        addedImage <<= image1
        addedImage += image2

        addedExposure = exposure1.Factory(addedImage, exposure1.getWcs())
        return addedExposure


        
        

