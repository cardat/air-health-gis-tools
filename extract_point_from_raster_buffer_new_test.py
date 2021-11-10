"""
THIS TEST SCRIPT IS SUPERCEDED BY THE NEW SCRIPT:  extract_gridpoints_from_rasterbuffer.py

Simple test script for optimising of raster file conversion to reference grid with raster stats extraction for given buffer around each grid point.
This method makes use of fast convolution (for buffering) and image reprojection using nearest neighbor for coordinate matching.

Note: resampleing with nearest neighbor is fastest method but on can also use linear interpolation.

Restrictions: Destination raster grid and input data grid must be regular grids.

To Do:

- Incl loop over multiple buffers (currently only largest buffer at 10000m)
- Include filenames via input arguments (currently static hard-coded for tst)
- include automatic crs check and transfromation to destination grid crs (in this example both use same crs)
- test extracted stats
- optimsation possible via processing multiple buffers in parallel and not exporting/importing all intermediate step rasters \
(currently incl here for testing)


Author: Sebastian Haan, Nathanial Butterworth


"""


import argparse
import os
from pathlib import Path
import numpy as np
import rasterio
import re
import glob
import time
import scipy
from astropy.convolution import convolve, Tophat2DKernel
#import pyreadr
from rasterio.warp import calculate_default_transform, reproject, Resampling

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


def buffer_convolve2(x,buffer):
	# use default uniform circle (Tophat2DKernel) rather than custom mask, need nromalization
	kernel = Tophat2DKernel(buffer) 
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



"""Arguments (need to be included in script below)

ap = argparse.ArgumentParser()
# ap.add_argument("-f","--file", default = "./data/ABS1x1km_Aus_Pop_Grid_2006_2020/data_provided/*.tif", type=Path)
# ap.add_argument("-g","--grid", default = "./data/AUS_points_5km.rds", type=Path)
# ap.add_argument("-o","--output", default = "./output", type=Path)
ap.add_argument("-f","--file", default = "./data/layers/ABS1x1km_Aus_Pop_Grid_2006_2020/data_provided/*.tif", type=Path)
ap.add_argument("-g","--grid", default = "./data/grids/nsw_points_1km_test.rds", type=Path)
ap.add_argument("-o","--output", default = "./output", type=Path)
args = ap.parse_args()

"""



############

if __name__ == "__main__":

	"""
	## Read in population rasters
	poprasts = glob.glob(str(args.file))

	#poprasts=["ABS1x1km_Aus_Pop_Grid_2006_2020/data_provided/apg06e_f_001_20210512.tif"]#,
	#			"ABS1x1km_Aus_Pop_Grid_2006_2020/data_provided/apg09e_f_001_20210512.tif"]

	buffs = [700, 1000, 1500, 2000, 3000, 5000, 10000]

	t1=time.time()
	print("Reading points rds file...")
	grid = pyreadr.read_r(str(args.grid))
	grid=list(grid.items())[0][1]
	#grid=grid.iloc[0:100, :]
	# rename 'x','y' to 'X' and 'Y'
	if ('x' in list(grid)) & ('y' in list(grid)):
		grid.rename(columns = {'x':'X', 'y':'Y'}, inplace = True)
	"""

	# Simple manual test, incl proper i/o name from CML arguments (TBD) 

	buff = 10000

	t0=time.time()
	print("Reading in files...")
	gridfile = rasterio.open('data/grid_to_do_APMMA_NSW_20211018.tif')
	grid_transform = gridfile.transform
	grid_crs=gridfile.crs
	grid_width=gridfile.width
	grid_height=gridfile.height
	grid = gridfile.read()
	grid = grid[0]
	grid_nodata = gridfile.nodata
	grid_xres, grid_yres = gridfile.res
	gridfile.close()

	datafile = rasterio.open('data/apg18e_1_0_0_20210512_crs3577.tif')
	data_nodata = datafile.nodata
	data = datafile.read()
	data = data[0]
	data[data == data_nodata] = np.nan
	data_xres, data_yres = datafile.res
	b=np.ceil(buff/data_xres)
	t1=time.time()
	print("Convolution...")
	density_array = buffer_convolve(data,b)
	t2=time.time()
	print("Convolution time: ", np.round(t2-t1,2))

	print("saving convolved array...")

	with rasterio.open('data/data_conv.tif',
		'w',
		driver='GTiff',
		height=datafile.height,
		width=datafile.width,
		count=1,
		dtype=data.dtype,
		crs=datafile.crs,
		nodata = grid_nodata,
		transform=datafile.transform,) as dest_file:
		dest_file.write(density_array, 1)
	dest_file.close()
	datafile.close()
	

	t3=time.time()

	print("Converting to grid coordinates...")

	with rasterio.open('data/data_conv.tif') as src:
		kwargs = src.meta.copy()
		kwargs.update({'crs': grid_crs,'transform': grid_transform, 'width': grid_width,'height': grid_height})

		with rasterio.open('data/data_conv_warp.tif', 'w', **kwargs) as dst:
			reproject(source=rasterio.band(src, 1), destination=rasterio.band(dst, 1),
				src_transform=src.transform,
				src_crs=src.crs,
				dst_transform=grid_transform,
				dst_crs=grid_crs,
				dst_width=grid_width,
				dst_height=grid_height, 
				dst_nodata=grid_nodata,
				src_nodata=src.nodata,
				resampling=Resampling.nearest)

	t4=time.time()
	print("Coordinate Conversion time: ", np.round(t4-t3,2))

	# mask image
	with rasterio.open('data/data_conv_warp.tif') as src:
		data = src.read()[0]
		data[grid == grid_nodata] = grid_nodata
		#data[grid == 0] = grid_nodata
		kwargs = src.meta.copy()
		with rasterio.open('data/data_conv_warp_masked.tif','w', **kwargs) as dest_file:
			dest_file.write(data, 1)
	dest_file.close()

	t5=time.time()

	print("Summary Time [seconds]")
	print("----------------------")
	print("Reading Files: ", np.round(t1-t0,2))
	print("Convolution: ", np.round(t2-t1,2))
	print("Export Convolved: ", np.round(t3-t2,2))
	print("Coordinate Conversion: ", np.round(t4-t3,2))
	print("Masking and final Export: ", np.round(t5-t4,2))
	print("TOTAL TIME: ", np.round(t5-t0,2))
