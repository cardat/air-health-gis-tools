import argparse
from pathlib import Path

import sys
import numpy as np
import pandas as pd
import pyreadr
import geopandas as gpd
from osgeo import gdal
import rasterstats as rs

"""Arguments"""

ap = argparse.ArgumentParser()
ap.add_argument("-f","--file", default = "./data/layers/ABS1x1km_Aus_Pop_Grid_2006_2020/data_provided/*.tif", type=Path,help="One or more raster layers selected for data extraction.")
ap.add_argument("-g","--grid", default = "./data/grids/testing_points.rds", type=Path, help="Dataframe with FID and X Y locations to extract.")
ap.add_argument("-o","--output", default = "./output", type=Path)
args = ap.parse_args()

def open_gdal(filename):
	#Open a raster file using gdal and set it up
	gdal_data = gdal.Open(filename)
	# gdal_band = gdal_data.GetRasterBand(1)
	# nodataval = gdal_band.GetNoDataValue()
	array_gdal = gdal_data.ReadAsArray().astype(np.float)
	# gt = gdal_data.GetGeoTransform()
	# wkt = gdal_data.GetProjection()
	# if np.any(array_gdal == nodataval):
	# 	array_gdal[array_gdal == nodataval] = np.nan

	# gdal_data.FlushCache()
	# gdal_data = None
	# del gdal_data

	#Return the raster array and the spatial details about the raster
	return(array_gdal)

# test = open_gdal(str(args.file))
# print(test)

print("Reading points rds file...")
grid = pyreadr.read_r(str(args.grid))
grid=list(grid.items())[0][1]

size = sys.getsizeof(grid)

print("This grid takes up: ",size, " bytes when loaded into python.")

test_raster = open_gdal(str(args.file))

print("The raster named ",str(args.file), " takes up ", sys.getsizeof(test_raster), " bytes")