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
import matplotlib.pyplot as plt

import swifter

#Python base libraries
import re
import glob
import multiprocessing as mp
import time

#conda install -c conda-forge gdal
#pip install arcgis
#conda install -c esri arcgis



#RASTERIO
# tic=time.time()
# src = rasterio.open(folder+tiffile)
# array_rasterio = src.read(1)
# nodataval= src.nodatavals

# if np.any(array_rasterio == nodataval):
    # array_rasterio[array_rasterio == nodataval] = np.nan
    
# print("Read Raster with Rasterio", time.time() - tic)
# print(np.shape(array_rasterio))



def get_coords_at_point(gt, lon, lat):
    row = int((lon - gt[0])/gt[1])
    col = int((lat - gt[3])/gt[5])
    return (col, row)





##############

def points_in_circle(circle, arr):
    '''
    A generator to return all points whose indices are within given circle.
    http://stackoverflow.com/a/2774284
    Warning: If a point is near the the edges of the raster it will not loop 
    around to the other side of the raster!
    '''
    i0,j0,r = circle
    

    for i in range(intceil(i0-r),intceil(i0+r)):
        ri = np.sqrt(r**2-(i-i0)**2)
        for j in range(intceil(j0-ri),intceil(j0+ri)):
            if (i >= 0 and i < len(arr[:,0])) and (j>=0 and j < len(arr[0,:])):               
                yield arr[i][j]

#            
def intceil(x):
    return int(np.ceil(x))                                            

#
def coregRaster(j0,i0,data,region):
    '''
    Finds the mean value of a raster, around a point with a specified radius.
    point - array([longitude,latitude])
    data - array
    region - integer, same units as data
    '''
    pts_iterator = points_in_circle((i0,j0,region), data)
    pts = np.array(list(pts_iterator))

	#Count area that contributed to calc
    squares= np.count_nonzero(~np.isnan(pts))

    return(np.nansum(pts),squares)



def neighbour_distance(gdf_chunk):
    #pop=gdf_chunk.mean()
	#pop=gdf_chunk.apply(lambda x: coregRaster(x[0], x[1],array_gdal,7), axis=1)
	pop = np.array(map(coregRaster, x[0], x[1],array_gdal,7))
	return(pop)

#def neighbour_distance1(gdf_chunk):
#    return(gdf_chunk.mean())


#####################


#######
def parallelize():
	tic=time.time()
	cpus = mp.cpu_count()
	pool = mp.Pool(processes=cpus)
	print("Opened pool", time.time() - tic)
	print(pool)

	intersection_chunks = np.array_split(sjer_plots_points.ind, cpus)

	print(intersection_chunks[0])
	print(np.shape(pd.DataFrame(intersection_chunks[0])))

	tic=time.time()
	#[print(chunk) for chunk in intersection_chunks]
	print("running parallel...")
	#intersection_results = pd.concat(pool.map(neighbour_distance, ([0,1],[1,2],[3,4])))
	chunk_processes = [pool.apply_async(coregRaster, args=(x[0], x[1],array_gdal,7)) for chunk in intersection_chunks]
	print("Apply-ed Pool", time.time() - tic)

	pool.close()
	pool.join()

	#intersection_results = [chunk.get() for chunk in chunk_processes]

	print(np.shape(intersection_results))

	return(intersection_results)


if __name__ == '__main__':
    
	print("Running...")
	tic=time.time()
	pointsfile="AUS_points_5km.shp"
	sjer_plots_points = gpd.read_file(pointsfile)
	sjer_plots_points=sjer_plots_points.iloc[::10, :]
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



	#tic=time.time()
	#pop_area = sjer_plots_points.apply(lambda x: coregRaster(x.ind[0], x.ind[1],array_gdal,7), axis=1)
	#print("Coregister raster, determine pop and area", time.time() - tic)
	#print(np.shape(pop_area))

	#tic=time.time()
	#pop_area2 = sjer_plots_points.swifter.set_npartitions(16).apply(lambda x: coregRaster(x.ind[0], x.ind[1],array_gdal,7), axis=1)
	#print("Swifted raster, determine pop and area", time.time() - tic)
	#print(np.shape(pop_area2))

	cpus = mp.cpu_count()
	print(cpus)

	pool = mp.Pool(processes=cpus)
	pool.close

	tic=time.time()

	intersection_results = parallelize()
	print("Done the parallel part", time.time() - tic)
		
	print(intersection_results)