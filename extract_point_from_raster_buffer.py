import argparse
from pathlib import Path
from osgeo import gdal

import numpy as np
import pandas as pd
import rasterio
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
#from scipy.signal import convolve2d
#from scipy.ndimage import convolve
#use atropy convolution to deal with nans
from astropy.convolution import convolve
import pyreadr

print("Imported modules successfully.")

def buffer_convolve(x,buffer):
	#Make a panning window/kernel represented by 1/0 circle array
	kernel = create_buffer(buffer)
	#print(np.shape(kernel))

	#Run the convolution over the entire array with the buffer kernel
	neighbor_sum = convolve(x, kernel, boundary='fill', fill_value=0,
		normalize_kernel=False,nan_treatment='fill',preserve_nan=True)
	#Sum up the number of cells used in the kernel (to find area)
	num_neighbor = np.count_nonzero(kernel)

	#Return the density (sum/area)
	return(neighbor_sum/num_neighbor)

def create_buffer(r, center=None):
	#r is in index units of the array you want to buffer
	#Create a circular buffer r units in radius
	#Make an list of indexes 
	Y,X = np.ogrid[0:2*r-1, 0:2*r-1]
	dist_from_center = np.sqrt((X-r+1)**2 + (Y-r+1)**2)+1
	mask = dist_from_center <= r
	#print(mask*1)
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

	#Question - do you count the entire area (e.g. including 
	#the ocean) within the buffer or just the valid points.
	#Does a nan equal a 0 when considering area?
	return(np.nansum(pts)/squares)

def open_gdal(filename):
	#Open a raster file using gdal and set it up
	gdal_data = gdal.Open(filename)
	gdal_band = gdal_data.GetRasterBand(1)
	nodataval = gdal_band.GetNoDataValue()
	array_gdal = gdal_data.ReadAsArray().astype(float)
	gt = gdal_data.GetGeoTransform()
	wkt = gdal_data.GetProjection()
	if np.any(array_gdal == nodataval):
		array_gdal[array_gdal == nodataval] = np.nan

	gdal_data.FlushCache()
	gdal_data = None
	del gdal_data



	#Return the raster array and the spatial details about the raster
	return(array_gdal,gt,wkt,gdal_band,nodataval)

def cartesian_product(*arrays):
	'''
	Turns two vectors of x and y points into an array
	'''
	la = len(arrays)
	dtype = np.result_type(*arrays)
	arr = np.empty([len(a) for a in arrays] + [la], dtype=dtype)
	for i, a in enumerate(np.ix_(*arrays)):
		arr[...,i] = a
	return arr.reshape(-1, la)

def write_raster(new_array,gt,wkt,gdal_band,nodataval,output_filename):

	# Create gtif file
	driver = gdal.GetDriverByName("GTiff")
	output_file = output_filename

	dst_ds = driver.Create(output_file,
						   gdal_band.XSize,
						   gdal_band.YSize,
						   1,
						   gdal.GDT_Int16)


	#writting output raster
	dst_ds.GetRasterBand(1).WriteArray( new_array )
	#setting nodata value
	dst_ds.GetRasterBand(1).SetNoDataValue(nodataval)
	#setting extension of output raster
	# top left x, w-e pixel resolution, rotation, top left y, rotation, n-s pixel resolution
	dst_ds.SetGeoTransform(gt)
	# setting spatial reference of output raster
	srs = osr.SpatialReference()
	srs.ImportFromWkt(wkt)
	dst_ds.SetProjection( srs.ExportToWkt() )
	dst_ds = None
	print("wrote raster")
	#Close output raster dataset


def array2tree(array_gdal,gt):
	#Make a scipy KDTree from an array

	#Create the index vector in the x/row-coordinate
	startx=gt[0]
	stepx=gt[1]
	endx=startx+(np.shape(array_gdal)[1]+1)*stepx
	xx = np.arange(startx,endx,stepx)

	#Create the index vector in the y/column-coordinate
	starty=gt[3]
	stepy=gt[5]
	endy=starty+(np.shape(array_gdal)[0]+1)*stepy
	yy = np.arange(starty,endy,stepy)

	#Make XY-vector of all the combinations of the x-y coordinates
	arr = cartesian_product(xx, yy)

	print("Shape of index array:",np.shape(arr),np.shape(xx),np.shape(yy))
	
	tic=time.time()
	#N Points = Time (s)
	#100,000 = 1s
	#1,000,000 = 7s
	#5,000,000 = 70s
	#10,000,000 = 224s
	#30,000,000 = 600s
	tree = scipy.spatial.KDTree(arr)
	print("Made tree from gdal in",time.time()-tic)
	return(tree)

"""Constants and Environment"""

ap = argparse.ArgumentParser()
ap.add_argument("-f","--file", default = "./data/layers/ABS1x1km_Aus_Pop_Grid_2006_2020/data_provided/*.tif", type=Path)
ap.add_argument("-g","--grid", default = "./data/grids/100_testing_points.rds", type=Path)
ap.add_argument("-o","--output", default = "./output", type=Path)
args = ap.parse_args()

gdal.UseExceptions()

@delayed
def poprast_prep(pth,grid,buffs,gt0):
	#Read in current raster
	print("Running on", pth) 
	array_gdal, gt,wkt,gdal_band,nodataval = open_gdal(pth)
	print("Shape of array_gdal:",np.shape(array_gdal))

	#If this raster is different the first raster we must re build our trees/indexes
	if gt != gt0:
		print("Different raster extent detected")
		# #Make the tree
		# # tree=array2tree(array_gdal,gt)

		####Index with tree
		# tic=time.time()
		# _, indexes = tree.query(g)
		# print("Queried tree in",time.time()-tic)
		# tic=time.time()
		# arrind = list(zip(*np.unravel_index(indexes,np.shape(array_gdal))))
		# dfind=pd.Series(arrind)
		# grid['ind'] = dfind 
		# print("Zipped index index in",time.time()-tic)
		
		#print(grid["ind"])

		####Index with loop
		t1=time.time()
		print("Read",pth[-25:-20],",finding indexes...")
		ind=[]
		for row in grid.itertuples():
			#print(row.geometry.x, row.geometry.y)
			ind.append(get_coords_at_point(gt, row.X, row.Y))

		grid["ind"]=ind
		#print(grid["ind"])

		print("Indexed points loop", pth[-25:-20], "Time:",np.round(time.time()-t1,2),"s")

		####Index with apply
		#loop is 10x faster than apply
		#t1=time.time()
		#grid["ind"] = grid.apply(lambda x: get_coords_at_point(gt, x.geometry.x, x.geometry.y), axis=1)
		#print(grid["ind"])
		#print("Indexed points", pth, "Time:",time.time()-t1)

    #Each buffer will be added to a list, then convert to a dataframe
	poplist = []
	for buff in buffs:
		t1=time.time()   
		#Set buffer to index units
		b=np.ceil(buff/gt[1])

		#Run the convolution with the buffer
		density_array = buffer_convolve(array_gdal,b)
		
        #Write out the buffered raster, if you want.
		#write_raster(density_array,gt,wkt,gdal_band,nodataval)
        
		#Find the value of the convolved array at each point of interest
		pop = [density_array[x] for x in grid["ind"]]
        
        #Add it to the buffer-list.
		poplist.append(pop)
         
		print("Done pop! Buffer:", buff, "Buffer (index):", b, pth[-25:-20], "Time:",np.round(time.time()-t1,2),"s")


    #Add additional header
	yr = str(20) + re.sub(".*(apg|APG)(\\d{2}).*", "\\2", pth)
	#print(poplist)
	popdf = gpd.GeoDataFrame(poplist, index = ["popdens" + str(b) for b in buffs]).T
	popdf.insert(0, 'FID', grid.FID)
	popdf.insert(0, 'year', yr)

	#Free up mem
	gdal_data=None

	return(popdf)


############

if __name__ == "__main__":

	## Read in population rasters
	poprasts = glob.glob(str(args.file))

	#poprasts=["ABS1x1km_Aus_Pop_Grid_2006_2020/data_provided/apg06e_f_001_20210512.tif"]#,
	#			"ABS1x1km_Aus_Pop_Grid_2006_2020/data_provided/apg09e_f_001_20210512.tif"]

	buffs = [700, 5000, 10000]

	t1=time.time()
	print("Reading points rds file...")
	grid = pyreadr.read_r(str(args.grid))
	grid=list(grid.items())[0][1]
	# grid=grid.iloc[0:100, :]

	##Shapefile
	#grid = gpd.read_file('AUS_points_1km.shp')
	#g = np.column_stack((grid.geometry.x.to_list(),grid.geometry.y.to_list()))
	#grid = pd.DataFrame(g,columns=["X","Y"])
	#grid.insert(0, 'FID', range(1, len(grid) + 1))


	#For tree
	#g = np.column_stack((grid.geometry.x.to_list(),grid.geometry.y.to_list()))
	#print("Stacked points in",time.time()-tic)
	
	print("Done in ", np.round(time.time()-t1,2),"s",np.shape(grid))

	## Get the raster information from the first grid

	array_gdal, gt,_,_,_ = open_gdal(poprasts[0])

	print("hi")

	#Make a KD tree (assuming all tifs will have the same dimensions;
	# if not, the tree will be re-built on each loop through the raster).
	#tree=array2tree(array_gdal,gt)

	t1=time.time()
	print("Finding indexes...for grid")
	ind=[]
	for row in grid.itertuples():
		#print(row.geometry.x, row.geometry.y)
		ind.append(get_coords_at_point(gt, row.X, row.Y))

	grid["ind"]=ind
	print("Indexed points loop:", np.round(time.time()-t1,2),"s")

	#Create the dask delayed object to submit to multiple workers
	#Or loop through the rasters and run
	t = []
	for pth in poprasts:
		#For dask:
		t.append(poprast_prep(pth,grid,buffs,gt))
		#No dask:
		# dd=poprast_prep(pth,grid,buffs,gt)

	#Run compute with dask
	print("Finished appending, running dask compute...")
	tic=time.time()
	dd=compute(t,scheduler="multiprocessing")
	#Combine all the runs
	t=pd.concat(dd[0])
	print("Done dask:",np.round(time.time()-t1,2),"s")

	

	#Save the result to a file
	output_fnm = args.file.parts
	grid_fnm = args.grid.parts
	outfile=str(args.output) + "/" + str(output_fnm[2]) + "_extracted_" + str(grid_fnm[2]) +".rds"
	pyreadr.write_rds(outfile, t, compress="gzip")
	print("Finished and saved output to:", outfile)
