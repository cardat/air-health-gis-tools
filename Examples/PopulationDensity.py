import numpy as np
import pandas as pd
import rasterio
import geopandas as gpd
from osgeo import gdal
import rasterstats as rs
import re
import glob
from numba import jit
import dask #.bag as db
from dask import delayed, compute
import time
#import multiprocessing
#pool = multiprocessing.Pool(processes=2)



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
										  

def coregRaster(i0,j0,data,region):
	'''
	Coregisters a point with a buffer region of a raster. 

	INPUTS
	i0: column-index of point of interest
	j0: row-index of point of interest
	data: two-dimensional numpy array (raster)
	region: integer, same units as data resolution

	RETURNS
	pts: all values from array within region


	'''
	pts_iterator = points_in_circle((i0,j0,region), data)
	pts = np.array(list(pts_iterator))

	#Count area that contributed to calc
	squares= np.count_nonzero(~np.isnan(pts))
	#each square is 1000m x 1000m
	#each index unit is 1000m
	
	#Density = total vol / area 
	#area = no. squares 

	#Question - do you count the entire area or just the valid points
	#Does a nan equal a 0 when considering area?
	return(np.nansum(pts)/squares)

#def convertKM2units(gt):
		#figure out conversion from km to grid units


@delayed
def poprast_prep(pth,grid,buffs):
		print("Running on", pth) 

		#Open the raster file and set it up
		gdal_data = gdal.Open(pth)
		gdal_band = gdal_data.GetRasterBand(1)
		nodataval = gdal_band.GetNoDataValue()
		array_gdal = gdal_data.ReadAsArray().astype(np.float)
		gt = gdal_data.GetGeoTransform()
		if np.any(array_gdal == nodataval):
			array_gdal[array_gdal == nodataval] = np.nan

		#Create an empty array to store the sampled points stats in.
		pop=np.empty(len(grid))
		pop[:] = np.NaN

		poplist=[]
		for buff in buffs:
			#Set the buffer size (in m) to index units
			print("Registering",pth[-26:-20],buff)
			b=np.ceil(buff/gt[1])

			tic=time.time()
			for i,row in enumerate(grid.itertuples()):
				ind=get_coords_at_point(gt, row.geometry.x, row.geometry.y)
				pop[i]=coregRaster(ind[0], ind[1], array_gdal,b)
			
			#print(pop)
			poplist.append(pop.tolist())
			print("Done pop",pth[-26:-20],buff,time.time()-tic)
		
		yr = str(20) + re.sub(".*(apg|APG)(\\d{2}).*", "\\2", pth)

		popdf = gpd.GeoDataFrame(poplist, index = ["popdens" + str(b) for b in buffs]).T
		popdf.insert(0, 'FID', grid.FID)
		popdf.insert(0, 'year', yr)

		#Close gdal file
		gdal_data=None

		return(popdf)

############

if __name__ == "__main__":

	## Read in population rasters
	#poprasts = glob.glob('ABS1x1km_Aus_Pop_Grid_2006_2020/' +
    #                 'data_provided/*.tif')

	poprasts=["ABS1x1km_Aus_Pop_Grid_2006_2020/data_provided/apg06e_f_001_20210512.tif",
				"ABS1x1km_Aus_Pop_Grid_2006_2020/data_provided/apg09e_f_001_20210512.tif"]

	buffs = [700] #, 1000, 1500, 2000, 3000, 5000, 10000]

	tic=time.time()
	print("Reading points shapefile...")
	grid = gpd.read_file('AUS_points_5km.shp')
	## Add an FID in there - if one doesn't already exist
	grid.insert(0, 'FID', range(1, len(grid) + 1))
	print("Done in ", time.time()-tic)

	t = []
	for pth in poprasts:
		t.append(poprast_prep(pth,grid,buffs))

	print("Finished appending, running compute...")
	tic=time.time()
	dd=compute(t,scheduler='processes', num_workers=2)
	print("Done dask:",time.time()-tic)


	t=pd.concat(dd[0])


	#print(f'Time: {time.time() - start}')

	t.to_csv('popdensity3.csv')

### Convert to RDS in R
