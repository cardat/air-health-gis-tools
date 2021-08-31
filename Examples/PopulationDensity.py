import numpy as np
import pandas as pd
import rasterio
import geopandas as gpd
from osgeo import gdal
import rasterstats as rs
import re
import glob
from numba import jit
import dask.bag as db

## To see how long it takes
import time


## Read in population rasters
poprasts = glob.glob('ABS1x1km_Aus_Pop_Grid_2006_2020/' +
                     'data_provided/*.tif')

#poprasts=glob.glob("ABS1x1km_Aus_Pop_Grid_2006_2020/data_provided/" +
#                     "apg06e_f_001_20210512.tif")

tic=time.time()
print("Reading points shapefile...")
#grid = gpd.read_file('singlepoint.shp')
grid = gpd.read_file('AUS_points_5km.shp')

print("Done in ", time.time()-tic)
## Add an FID in there - if one doesn't already exist
grid.insert(0, 'FID', range(1, len(grid) + 1))

#grid=grid.iloc[::100, :]



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

	return(np.nansum(pts)/squares)

#def convertKM2units(gt):
		#figure out conversion from km to grid units

#@delayed
def popbuff(buff):
	#gdal_data = gdal.Open(pth)
	gdal_band = gdal_data.GetRasterBand(1)
	nodataval = gdal_band.GetNoDataValue()
	array_gdal = gdal_data.ReadAsArray().astype(np.float)
	gt = gdal_data.GetGeoTransform()
	if np.any(array_gdal == nodataval):
		array_gdal[array_gdal == nodataval] = np.nan

	#Create an empty array to store the results in.
	pop=np.empty(len(grid))
	pop[:] = np.NaN

	#tic=time.time()
	#grid["ind"] = grid.apply(lambda x: get_coords_at_point(gt, x.geometry.x, x.geometry.y), axis=1)
	#print("Done indexing ", time.time()-tic)
	#for i, row in enumerate(grid.itertuples()):
		#print(i)
	#	ind = get_coords_at_point(gt, grid.loc[i].geometry.x, grid.loc[i].geometry.y)
	
	#Set the buffer size (in m) to index units
	b=np.ceil(buff/gt[1])
	#b=buff
	#tic=time.time()
	#for i,row in enumerate(grid.itertuples()):
	#for i in range(len(grid)):
	#	pop[i]=coregRaster(grid.loc[i,"ind"][0], grid.loc[i,"ind"][1], array_gdal,b)

	#print("Done pop range", time.time()-tic) #~14 sec for full 5km

	#Find the buffered population density at each point in the grid.
	tic=time.time()
	for i,row in enumerate(grid.itertuples()):
		#pop[i]=coregRaster(row.ind[0], row.ind[1], array_gdal,b)
		ind=get_coords_at_point(gt, row.geometry.x, row.geometry.y)
		pop[i]=coregRaster(ind[0], ind[1], array_gdal,b)

	#print("Done pop itertuples ", time.time()-tic) #
	
	#tic=time.time()
	#Find the buffer size in raster-index units
	#gt[1] assumes x and y are the same size 
	
	#pop = grid.apply(lambda x: coregRaster(x.ind[0], x.ind[1],array_gdal,b), axis=1)
	print("Done pop ", time.time()-tic)

	return(pop)
	

## Define a 'sumna' function that removes missing, and any negative values
def sumna(x):
	return np.nansum(x[x>0])

## Function for one year's extract
def popbuffOLD(buff):
	b = grid.geometry.buffer(buff)
	pop = rs.zonal_stats(b,
						 poprast.read(1),
						 nodata = 0,
						 affine = poprast.transform,
						 stats = 'sum',
						 add_stats = {'sumna' : sumna})
	out = gpd.GeoDataFrame(pop)['sumna']
	out = out/(b.area/1e6)
	return out


buffs = [700, 1000, 1500, 2000, 3000, 5000, 10000]
bag=db.from_sequence(buffs)

t = []
start = time.time()



#Dask or apply this
for pth in poprasts:
		print("Running on", pth) 
		#with gdal.Open(pth) as gdal_data:
		gdal_data = gdal.Open(pth)
		yr = str(20) + re.sub(".*(apg|APG)(\\d{2}).*", "\\2", pth)
		popdf = list(map(popbuff, buffs))
		#popdf = bag.map(popbuff).compute()
		popdf = gpd.GeoDataFrame(popdf, index = ["popdens" + str(b) for b in buffs]).T
		popdf.insert(0, 'FID', grid.FID)
		popdf.insert(0, 'year', yr)
		t.append(popdf)
		#Close gdal file
		gdal_data=None

#t = dask.delayed(sum)(t)

t = pd.concat(t)
t2 = pd.DataFrame(t)

print(f'Time: {time.time() - start}')

t2.to_csv('popdensity2.csv')

### Convert to RDS in R
