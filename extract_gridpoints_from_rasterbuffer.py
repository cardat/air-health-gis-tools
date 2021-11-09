"""
Simple test script for optimising of raster file conversion to reference grid with raster stats extraction for given buffer around each grid point.
This method makes use of fast convolution with nan-integration (for buffering) and image reprojection using nearest neighbor for coordinate macthing.

Restrictions: Destionation raster grid and input data grad must be regular grids


Notes: 
- buffers ar currenmtly hardcoded at buffers = [700, 1000, 1500, 2000, 3000, 5000, 10000]
(Change in main function if needed)
- resampling with nearest neighbor is fastest method but on can also use linear interpolation 
(for change replace in code  resampling=Resampling.nearest with other method)


To Do:

- include automatic crs check and transfromation to destination grid crs (in this example both use same crs)
- test extracted stats
- save transformation matrix for reprojection once and reuse for each buffer (--> will significantly speed up computation)
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


#Arguments 

ap = argparse.ArgumentParser()
# ap.add_argument("-f","--file", default = "./data/ABS1x1km_Aus_Pop_Grid_2006_2020/data_provided/*.tif", type=Path)
# ap.add_argument("-g","--grid", default = "./data/AUS_points_5km.rds", type=Path)
# ap.add_argument("-o","--output", default = "./output", type=Path)
ap.add_argument("-f","--file", default = "./data/grid_to_do_APMMA_NSW_20211018.tif", type=Path)
ap.add_argument("-g","--grid", default = "./data/apg18e_1_0_0_20210512_crs3577.tif", type=Path)
ap.add_argument("-o","--output", default = "./output", type=Path)
args = ap.parse_args()





############

if __name__ == "__main__":

	buffers = [700, 1000, 1500, 2000, 3000, 5000, 10000]

	fname_raster = str(args.file)
	fname_grid = str(args.grid)
	if not os.path.exists(fname_raster):
		print(f'{fname_raster} does not exist')
	if not os.path.exists(fname_grid):
		print(f'{fname_raster} does not exist')

	outpath = str(args.output)
	os.makedirs(outpath, exist_ok = True)



	# Simple manual test, incl proper i/o name from CML arguments (TBD) 

	#buff = 10000

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
	data_dtype = data.dtype
	data_height = datafile.height
	data_width = datafile.width
	data_crs = datafile.crs
	data_transform = datafile.transform
	datafile.close()

	# Temp files for convolution and reprojection
	fname_conv_temp = os.path.join(outpath,'data_conv_temp.tif')
	fname_warp_temp = os.path.join(outpath,'data_warp_temp.tif')


	for buff in buffers:
		b=np.ceil(buff/data_xres)
		t1=time.time()
		print(f"Processing with buffer {buff} meters ...")
		density_array = buffer_convolve(data,b)
		t2=time.time()
		#print("Convolution time: ", np.round(t2-t1,2))
		print("saving convolved array...")

		with rasterio.open(fname_conv_temp,
			'w',
			driver='GTiff',
			height=data_height,
			width=data_width,
			count=1,
			dtype=data_dtype,
			crs=data_crs,
			nodata = grid_nodata,
			transform=data_transform,) as dest_file:
			dest_file.write(density_array, 1)
		dest_file.close()
		
		t3=time.time()

		print("Converting to grid coordinates...")

		with rasterio.open(fname_conv_temp) as src:
			kwargs = src.meta.copy()
			kwargs.update({'crs': grid_crs,'transform': grid_transform, 'width': grid_width,'height': grid_height})

			with rasterio.open(fname_warp_temp, 'w', **kwargs) as dst:
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
		src.close()
		dst.close()

		t4=time.time()
		print("Coordinate Conversion time: ", np.round(t4-t3,2))

		# Output image name:
		fname_final_out = os.path.join(outpath, args.file.stem + '_buffer-' + str(buff) + 'm.tif')

		# mask image
		with rasterio.open(fname_warp_temp) as src:
			data_new = src.read()[0]
			data_new[grid == grid_nodata] = grid_nodata
			#data[data < grid_nodata] = grid_nodata
			#data[~np.isfinite(data)] = grid_nodata
			kwargs = src.meta.copy()
			with rasterio.open(fname_final_out,'w', **kwargs) as dest_file:
				dest_file.write(data_new, 1)
		src.close()
		dest_file.close()

		t5=time.time()


		print(f"Summary Time [seconds] for buffer {buff}m")
		print("------------------------------------------")
		#print("Reading Files: ", np.round(t1-t0,2))
		print("Convolution and export: ", np.round(t3-t1,2))
		#print("Export Convolved: ", np.round(t3-t2,2))
		print("Coordinate projection: ", np.round(t4-t3,2))
		print("Masking and final Export: ", np.round(t5-t4,2))
		print("------------------------------------------")

		# Deleting temp files:
		if os.path.exists(fname_conv_temp):
			os.remove(fname_conv_temp)
		if os.path.exists(fname_warp_temp):
			os.remove(fname_warp_temp)

	print("TOTAL TIME: ", np.round(t5-t0,2))