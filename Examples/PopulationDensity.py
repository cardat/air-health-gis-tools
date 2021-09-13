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

import scipy
from scipy.signal import convolve2d

from scipy.ndimage import convolve


def buffer_convolve2d(x,buffer):
    kernel = create_buffer(buffer)
    #print(kernel)
    
	#neighbor_sum = convolve2d(
    #    x, kernel, mode='same',
    #    boundary='fill', fillvalue=0)

    neighbor_sum = convolve(x, kernel, mode='constant', cval=0)
    num_neighbor = np.count_nonzero(kernel)
    #num_neighbor = convolve(np.ones(x.shape), kernel, mode='reflect', cval=0)

	
    #num_neighbor = convolve2d(
    #    np.ones(x.shape), kernel, mode='same',
    #    boundary='fill', fillvalue=0)

    return(neighbor_sum/num_neighbor)

def create_buffer(r, center=None, radius=None):

    if center is None: # use the middle of the image
        center = (int(r/2), int(r/2))
    if radius is None: # use the smallest distance between the center and image walls
        radius = min(center[0], center[1], r-center[0], r-center[1])

    Y, X = np.ogrid[:r, :r]
    dist_from_center = np.sqrt((X - center[0])**2 + (Y-center[1])**2)

    mask = dist_from_center <= radius
    return(mask*1.0)


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
	# print(lon, gt[0], gt[1])
	# print(lat, gt[3], gt[5])
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

def cartesian_product(*arrays):
    la = len(arrays)
    dtype = np.result_type(*arrays)
    arr = np.empty([len(a) for a in arrays] + [la], dtype=dtype)
    for i, a in enumerate(np.ix_(*arrays)):
        arr[...,i] = a
    return arr.reshape(-1, la)

#@delayed
def poprast_prep(pth,grid,buffs,g):

	print("Running on", pth) 

	#Open the raster file and set it up
	gdal_data = gdal.Open(pth)
	gdal_band = gdal_data.GetRasterBand(1)
	nodataval = gdal_band.GetNoDataValue()
	array_gdal = gdal_data.ReadAsArray().astype(np.float)
	gt = gdal_data.GetGeoTransform()
	if np.any(array_gdal == nodataval):
		array_gdal[array_gdal == nodataval] = np.nan


	###### this assumes each raster might be different 

	t1=time.time()

	print("Shape of array_gdal:",np.shape(array_gdal))

	startx=gt[0]
	stepx=gt[1]
	endx=startx+(np.shape(array_gdal)[1]+1)*stepx
	xx = np.arange(startx,endx,stepx)
	#xx=np.arange(startx,gt[0]+1000*7401,1000)
	
	starty=gt[3]
	stepy=gt[5]
	endy=starty+(np.shape(array_gdal)[0]+1)*stepy
	yy = np.arange(starty,endy,stepy)
	print(xx,yy)
	arr = cartesian_product(yy, xx)

	print("Shape of index array:",np.shape(arr),np.shape(xx),np.shape(yy))
	
	print(arr)
	print(g)
	
	tic=time.time()
	#100,000 = 1s
	#1,000,000 = 7s
	#5,000,000 = 70s
	#10,000,000 = 224s
	#30,000,000 = 600s
	tree = scipy.spatial.cKDTree(arr)
	print("Made tree from gdal in",time.time()-tic)

	tic=time.time()
	_, indexes = tree.query(g)
	print("Queried tree in",time.time()-tic)

	tic=time.time()
	arrind = list(zip(*np.unravel_index(indexes,np.shape(array_gdal))))
	dfind=pd.Series(arrind)
	grid['ind'] = dfind 
	print("Zipped index index in",time.time()-tic)
	
	print("Indexed points loop", pth[-26:-20], "Time:",time.time()-t1)

	#print(dfind)
	print(grid["ind"])

	#######33
	# t1=time.time()
	# print("Read",pth[-26:-20],",finding indexes...")
	ind=[]
	for row in grid.itertuples():
		print(row.geometry.x, row.geometry.y)
		ind.append(get_coords_at_point(gt, row.geometry.x, row.geometry.y))

	grid["ind"]=ind
	print(grid["ind"])
	# print("Indexed points loop", pth[-26:-20], "Time:",time.time()-t1)

	#loop is 10x faster than apply
	#t1=time.time()
	#grid["ind"] = grid.apply(lambda x: get_coords_at_point(gt, x.geometry.x, x.geometry.y), axis=1)
	#print(grid["ind"])
	#print("Indexed points", pth, "Time:",time.time()-t1)

	poplist = []
	for buff in buffs:
		#Set buffer to index units
		b=np.ceil(buff/gt[1])
		t1=time.time()
		#Run the convolution with the buffer
		y2_large = buffer_convolve2d(array_gdal,b)
		#Find the value of the convolved array at each point of interest
		pop = [y2_large[x] for x in grid["ind"]]
		poplist.append(pop)

		print("Done pop", buff,b, pth[-26:-20], "Time:",time.time()-t1)

	yr = str(20) + re.sub(".*(apg|APG)(\\d{2}).*", "\\2", pth)
	print(poplist)
	popdf = gpd.GeoDataFrame(poplist, index = ["popdens" + str(b) for b in buffs]).T
	popdf.insert(0, 'FID', grid.FID)
	popdf.insert(0, 'year', yr)

	gdal_data=None

	return(popdf)


############

if __name__ == "__main__":

	## Read in population rasters
	#poprasts = glob.glob('ABS1x1km_Aus_Pop_Grid_2006_2020/' +
    #                 'data_provided/*.tif')

	poprasts=["ABS1x1km_Aus_Pop_Grid_2006_2020/data_provided/apg06e_f_001_20210512.tif"]#,
				#"ABS1x1km_Aus_Pop_Grid_2006_2020/data_provided/apg09e_f_001_20210512.tif"]

	buffs = [700, 10000] #, 1500, 2000, 3000, 5000, 10000]

	tic=time.time()
	print("Reading points shapefile...")
	#grid = gpd.read_file('AUS_points_5km.shp')
	grid = gpd.read_file('point06.shp')

	tic=time.time()
	g = np.column_stack((grid.geometry.x.to_list(),grid.geometry.y.to_list()))
	print("Stacked points in",time.time()-tic)
	
	
	## Add an FID in there - if one doesn't already exist
	grid.insert(0, 'FID', range(1, len(grid) + 1))

	print("Done in ", time.time()-tic,np.shape(grid))

	t = []
	for pth in poprasts:
		t.append(poprast_prep(pth,grid,buffs,g))
		#dd=poprast_prep(pth,grid,buffs,g)

	#client = Client()

	print("Finished appending, running compute...")
	tic=time.time()
	dd=compute(t,scheduler="multiprocessing",num_workers=6)
	print("Done dask:",time.time()-tic)


	t=pd.concat(dd[0])


	#print(f'Time: {time.time() - start}')

	t.to_csv('pop1km.csv')

### Convert to RDS in R
