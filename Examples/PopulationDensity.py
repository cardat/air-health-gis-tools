import numpy as np
import pandas as pd
import rasterio
import geopandas as gpd
from osgeo import gdal
import rasterstats as rs
import re
import glob
from numba import jit

## To see how long it takes
import time


## Read in population rasters
poprasts = glob.glob('ABS1x1km_Aus_Pop_Grid_2006_2020/' +
                     'data_provided/*.tif')

tic=time.time()
print("Reading points shapefile...")
grid = gpd.read_file('AUS_points_5km.shp')
print("Done in ", time.time()-tic)
## Add an FID in there - if one doesn't already exist
grid.insert(0, 'FID', range(1, len(grid) + 1))



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

	return(np.nansum(pts)/squares)

def popbuff(buff):
	#gdal_data = gdal.Open(pth)
	gdal_band = gdal_data.GetRasterBand(1)
	nodataval = gdal_band.GetNoDataValue()
	array_gdal = gdal_data.ReadAsArray().astype(np.float)
	gt = gdal_data.GetGeoTransform()
	if np.any(array_gdal == nodataval):
		array_gdal[array_gdal == nodataval] = np.nan

	for row in grid.itertuples():
		ind = get_coords_at_point(gt, row.geometry.x, row.geometry.y)
		pop_area = coregRaster(ind[0], ind[1], array_gdal,7)

	return(pop_area)
	

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

buffs = [700, 1000] #, 1500, 2000, 3000, 5000, 10000]

t = []
start = time.time()
for pth in poprasts:
		print("Running on", pth) 
		#with gdal.Open(pth) as gdal_data:
		gdal_data = gdal.Open(pth)
		yr = str(20) + re.sub(".*(apg|APG)(\\d{2}).*", "\\2", pth)
		popdf = list(map(popbuff, buffs))
		popdf = gpd.GeoDataFrame(popdf, index = ["popdens" + str(b) for b in buffs]).T
		popdf.insert(0, 'FID', grid.FID)
		popdf.insert(0, 'year', yr)
		t.append(popdf)
		#Close gdal file
		gdal_data=None

t = pd.concat(t)
t2 = pd.DataFrame(t)

print(f'Time: {time.time() - start}')

t2.to_csv('popdensity2.csv')

### Convert to RDS in R
