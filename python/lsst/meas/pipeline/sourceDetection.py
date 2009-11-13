from lsst.pex.logging import Log

import lsst.pex.policy as policy
import lsst.afw.detection as afwDet
import lsst.afw.image as afwImg
import lsst.afw.math as afwMath
import lsst.meas.algorithms as measAlg


def makePsf(psfPolicy):
    params = []        
    params.append(psfPolicy.getString("algorithm"))
    params.append(psfPolicy.getInt("width"))
    params.append(psfPolicy.getInt("height"))
    if psfPolicy.exists("parameter"):
        params += psfPolicy.getDoubleArray("parameter")
        
    return measAlg.createPSF(*params)

def addExposures(exposureList):
    """
    Add a set of exposures together. 
    Assumes that all exposures in set have the same dimensions
    """
    exposure0 = exposureList[0]
    image0 = exposure0.getMaskedImage()

    addedImage = image0.Factory(image0, True)
    addedImage.setXY0(image0.getXY0())

    for exposure in exposureList[1:]:
        image = exposure.getMaskedImage()
        addedImage += image

    addedExposure = exposure0.Factory(addedImage, exposure0.getWcs())
    return addedExposure

def getBackground(image, backgroundPolicy):
    """
    Make a new Exposure which is exposure - background
    """
    algorithm = backgroundPolicy.get("algorithm")
    if algorithmName == "NATURAL_SPLINE":
        bctrl = afwMath.BackgroundControl(afwMath.NATURAL_SPLINE)
    else:
        raise RuntimeError, "Unknown backgroundPolicy.algorithm: %s" % algorithm            
    binsize = backgroundPolicy.get("binsize")

    
    #
    # Subtract background
    #
    bctrl.setNxSample(image.getWidth()/binsize + 1)
    bctrl.setNySample(image.getHeight()/binsize + 1)
    return afwMath.makeBackground(image, bctrl)
    
def subtractBackground(exposure, backgroundPolicy):
    maskedImage = exposure.getMaskedImage()
    
    deepCopy = maskedImage.Factory(maskedImage, True)
    deepCopy.setXY0(maskedImage.getXY0())

    image = deepCopy.getImage()    
    try:
        background = getBackground(image, backgroundPolicy)
    except:
        return exposure, None

    image -= backround.getImageF()
    del image

    backgroundSubtractedExposure = exposure.Factor(
        deepCopy, 
        afwImg.Wcs(exposure.getWcs)
    )

    return backgroundSubtractedExposure, background
    

def detectSources(exposure, psf, detectionPolicy):
    minPixels = detectionPolicy.get("minPixels")
    
    thresholdValue = detectionPolicy.get("thresholdValue")
    thresholdType = detectionPolicy.get("thresholdType")
    thresholdPolarity = detectionPolicy.get("thresholdPolarity")

    if exposure is None:
        raise RuntimeException("No exposure for detection")
       
    #
    # Unpack variables
    #
    maskedImage = exposure.getMaskedImage()
    region = afwImg.BBox(
        afwImg.PointI(maskedImage.getX0(), maskedImage.getY0()),
        maskedImage.getWidth(), 
        maskedImage.getHeight()
    )
   
    if not psf is None:
        convolvedImage = maskedImage.Factory(maskedImage.getDimensions())
        convolvedImage.setXY0(maskedImage.getXY0())

        # 
        # Smooth the Image
        #
        psf.convolve(convolvedImage, 
            maskedImage, 
            convolvedImage.getMask().getMaskPlane("EDGE")
        )
    
        #
        # Only search psf-smooth part of frame
        #
        llc = afwImg.PointI(
            psf.getKernel().getWidth()/2, 
            psf.getKernel().getHeight()/2
        )
        urc = afwImg.PointI(
            convolvedImage.getWidth() - 1,
            convolvedImage.getHeight() - 1
        )
        urc -= llc
        bbox = afwImg.BBox(llc, urc)    
        middle = convolvedImage.Factory(convolvedImage, bbox)
    else:
        middle = maskedImage

    dsNegative = None
    dsPositive = None

    if thresholdPolarity == "negative" or thresholdPolarity == "both":
        threshold = afwDet.createThreshold(
            thresholdValue,
            thresholdType,
            False
        )
        #detect negative sources
        dsNegative = afwDet.makeFootprintSet(
            middle,
            threshold,
            "DETECTED_NEGATIVE",
            minPixels
        )
        #set detection region to be the entire region
        dsPositive.setRegion(region)
        #
        # We want to grow the detections into the edge by at least one pixel so 
        # that it sees the EDGE bit
        #
        dsPositive = afwDet.FootprintSetF(dsPositive, 1, False)
        dsPositive.setMask(maskedImage.getMask(), "DETECTED_NEGATIVE")
    if thresholdPolarity != "negative":
        threshold = afwDet.createThreshold(
            thresholdValue,
            thresholdType,
            True
        )
        dsPositive = afwDet.makeFootprintSet(
            middle,
            threshold,
            "DETECTED",
            minPixels
        )
        #set detection region to be the entire region
        dsPositive.setRegion(region)
        #
        # We want to grow the detections into the edge by at least one pixel so 
        # that it sees the EDGE bit
        #
        dsPositive = afwDet.FootprintSetF(dsPositive, 1, False)
        dsPositive.setMask(maskedImage.getMask(), "DETECTED")

    #
    # clean up
    #
    del middle

    return dsPositive, dsNegative


