from utils import *

import argparse
from pathlib import Path

from osgeo import gdal, osr
import numpy as np

def open_gdal(filename):
	print("Where are you mr seg fault?")
	#Open a raster file using gdal and set it up
	gdal_data = gdal.Open(filename)
	print("hi ", gdal_data)
	gdal_band = gdal_data.GetRasterBand(1)
	nodataval = gdal_band.GetNoDataValue()
	array_gdal = gdal_data.ReadAsArray().astype(np.float)
	gt = gdal_data.GetGeoTransform()
	wkt = gdal_data.GetProjection()
	if np.any(array_gdal == nodataval):
		array_gdal[array_gdal == nodataval] = np.nan

	del gdal_data

	#Return the raster array and the spatial details about the raster
	return(array_gdal,gt,wkt,gdal_band,nodataval)


ap = argparse.ArgumentParser()
ap.add_argument("-f","--file", default = "./data/ABS1x1km_Aus_Pop_Grid_2006_2020/data_provided/apg06e_f_001_20210512.tif", type=Path)
args = ap.parse_args()

gdal.UseExceptions()

if __name__ == "__main__":

    print("Let's go hunting seg faults!")

    print("Does ", str(args.file), " cause a seg fault?")
    test = open_gdal(str(args.file))

    print(test, " doesn't cause a seg fault! yay :)")