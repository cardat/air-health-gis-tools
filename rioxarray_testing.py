import argparse
import os
from pathlib import Path
import numpy as np
import pandas as pd
import rasterio
import pyreadr
import rioxarray as riox
import geopandas as gpd
from osgeo import gdal, osr
import rasterstats as rs
import re
import glob
from numba import jit
import dask #.bag as db
from dask import delayed, compute
import time
import scipy

"""Arguments"""

ap = argparse.ArgumentParser()
# ap.add_argument("-f","--file", default = "./data/ABS1x1km_Aus_Pop_Grid_2006_2020/data_provided/*.tif", type=Path)
# ap.add_argument("-g","--grid", default = "./data/AUS_points_5km.rds", type=Path)
# ap.add_argument("-o","--output", default = "./output", type=Path)
ap.add_argument("-f","--file", default = "./data/layers/Impervious_Surface_NOAA_Satellite_2010/data_provided/impsa_2010_20210519.tif", type=Path)
ap.add_argument("-g","--grid", default = "./data/grids/nsw_points_1km_test.rds", type=Path)
ap.add_argument("-o","--output", default = "./output", type=Path)
args = ap.parse_args()


############

if __name__ == "__main__":

	# Define buffers
	buffs = [700, 1000, 1500, 2000, 3000, 5000, 10000]

	t1=time.time()
	print("Reading points rds file...")
	grid = pyreadr.read_r(str(args.grid))
	grid=list(grid.items())[0][1]
	#grid=grid.iloc[0:100, :]
	# rename 'x','y' to 'X' and 'Y'
	if ('x' in list(grid)) & ('y' in list(grid)):
		grid.rename(columns = {'x':'X', 'y':'Y'}, inplace = True)
		
	raster = riox.open_rasterio(str(args.file))
	
	print(raster)
