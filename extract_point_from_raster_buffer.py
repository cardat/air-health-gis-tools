#Geo libraries
import rasterio
import geopandas as gpd
from osgeo import gdal
from osgeo import osr
from osgeo import ogr 
import shapefile
import rasterstats as rs

#Standard libraries
import pandas as pd
import numpy as np
#import matplotlib.pyplot as plt

#Python base libraries
import re
import glob
import multiprocessing as mp
import time

import numba

#JIT directives applied to functions apply numbas'
#llvm compiler. That should speed stuff up.
from numba import jit

import swifter
from numba import cuda

import dask.dataframe as dd
from dask.multiprocessing import get

#conda install -c conda-forge gdal
#pip install arcgis
#conda install -c esri arcgis



#### Functions #####
## Will eventually move these to generic file for use by all
## extract tools.
####


@jit(nopython=True)
def get_coords_at_point(gt, lon, lat):
	'''
	Given a point in some coordinate reference (e.g. lat/lon)
	Find the closest point to that in an array (e.g. a raster)
	and return the index location of that point in the raster.
	
	INPUTS
	gt: output from "gdal_data.GetGeoTransform()"
	lon: x/row-coordinate of interest
	lat: y/column-coordinate of interest

	RETURNS
	col: y index value from the raster
	row: x index value from the raster
	'''

	row = int((lon - gt[0])/gt[1])
	col = int((lat - gt[3])/gt[5])
	return (col, row)


@jit(nopython=True)
def points_in_circle(circle, arr):
	'''
	A generator to return all points whose indices are within a given circle.
	http://stackoverflow.com/a/2774284
	Warning: If a point is near the the edges of the raster it will not loop 
	around to the other side of the raster!
	'''
	i0,j0,r = circle
	
	def intceil(x):
		return int(np.ceil(x))  

	for i in range(intceil(i0-r),intceil(i0+r)):
		ri = np.sqrt(r**2-(i-i0)**2)
		for j in range(intceil(j0-ri),intceil(j0+ri)):
			if (i >= 0 and i < len(arr[:,0])) and (j>=0 and j < len(arr[0,:])):               
				yield arr[i][j]        
										  

#
def coregRaster(i0,j0,data,region):
	'''
	Coregisters a point with a buffer region of a raster. 

	INPUTS
	i0: x/row-index of point of interest
	j0: y/column-index of point of interest
	data: two-dimensional numpy array (raster)
	region: integer, same units as data resolution

	RETURNS
	pts: all values from array within region


	'''
	pts_iterator = points_in_circle((i0,j0,region), data)
	pts = np.array(list(pts_iterator))

	#Count area that contributed to calc
	squares= np.count_nonzero(~np.isnan(pts))

	return(np.nansum(pts),squares)


#####################

if __name__ == '__main__':
	
	print("Running...")
	tic=time.time()
	pointsfile="AUS_points_5km.shp"
	sjer_plots_points = gpd.read_file(pointsfile)
	#sjer_plots_points=sjer_plots_points.iloc[::10, :]
	print("Read points", time.time() - tic)

	folder="ABS1x1km_Aus_Pop_Grid_2006_2020/data_provided/"
	tiffile="apg06e_f_001_20210512.tif"

	##GDAL
	tic=time.time()
	gdal_data = gdal.Open(folder+tiffile)
	gdal_band = gdal_data.GetRasterBand(1)
	nodataval = gdal_band.GetNoDataValue()
	array_gdal = gdal_data.ReadAsArray().astype(np.float)
	gt = gdal_data.GetGeoTransform()

	if np.any(array_gdal == nodataval):
		array_gdal[array_gdal == nodataval] = np.nan
		
	print("Read Raster with GDAL", time.time() - tic)
	print(np.shape(array_gdal))

	tic=time.time()
	sjer_plots_points["ind"] = sjer_plots_points.apply(lambda x: get_coords_at_point(gt, x.geometry.x, x.geometry.y), axis=1)
	print("Assigned index locations to points", time.time() - tic)
	


	tic=time.time()
	for row in sjer_plots_points.itertuples():
		pop_area = coregRaster(row.ind[0], row.ind[1],array_gdal,7)
	#pop_area = sjer_plots_points.apply(lambda x: coregRaster(x.ind[0], x.ind[1],array_gdal,7), axis=1)
	print("Coregister raster, determine pop and area", time.time() - tic)
	print(np.shape(pop_area))

	#tic=time.time()
	#pop_area2 = sjer_plots_points.swifter.set_npartitions(16).apply(lambda x: coregRaster(x.ind[0], x.ind[1],array_gdal,7), axis=1)
	#print("Swifted raster, determine pop and area", time.time() - tic)
	#print(np.shape(pop_area2))

	cpus = mp.cpu_count()
	print(cpus)

	pool = mp.Pool(processes=cpus)
	pool.close

	tic=time.time()

	#intersection_results = parallelize()
	print("Done the parallel part", time.time() - tic)
		
	#print(intersection_results)