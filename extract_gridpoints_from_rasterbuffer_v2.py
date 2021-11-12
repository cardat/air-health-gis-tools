"""
Script for extracting buffered raster stats averages from input raster at positions of destination grid points.

To optimise the buffer extraction this method makes use 1) of fast convolution incl, taking into accounts nan values,
and 2) taking advantage of the fact that destination points are distributed in regular grid, 
so we can take advantage of fitting a grid transformation matrix between original raster and destination raster
instead of using much slower triangulated interpolation for each point. 

Arguments:
-f or --file: path+filename of input raster (buffered stats exatracted from this)
-g or --grid: path+filename of destination grid raster
-o or --output: output pathname

HOW TO RUN:
python3 -f INPUTFILENAME -g DESTINATIONFILENAME -o OUTPATHNAME extract_gridpoints_from_rasterbuffer.py


Notes: 
- There are two ways to do the buffer extraction:
	- Method 1: 1st step convolution for each buffer, 2nd step projection 
	- Method 2::  1st step projection, 2nd step convolution for each buffer (typically slower if convolution at higher res arster)
	The final method is automatically chosen by code based on:
		a)  If buffer size is larger/smaller than raster resolution (Method1/Method2)
		b)  If raster resolution larger/smaller than grid resolution (Method2/Method1)
	- or user can force the fast compute option with first method (convolution first) setting fast_compute = True (in main function)
- buffers are currently hard-coded at buffers = [700, 1000, 1500, 2000, 3000, 5000, 10000]
(Change in main function if needed)
- resampling with nearest neighbor is fastest method but on can also use linear interpolation 
To change set parameter resampling_option to one of the following (default Resampling.bilinear):
	Resampling.nearest, 
	Resampling.bilinear,
	Resampling.cubic,
	Resampling.cubic_spline,
	Resampling.lanczos,
	Resampling.average


Possible improvements:
- include automatic crs check and transformation to destination grid crs (now taken into account by fitting transformation with rasterio)
- automated test of extracted stats for some test points
- save transformation matrix for reprojection once and reuse for each buffer (--> can speed up computation)


Tested with Python 3.9

Requirements:

numpy>=1.20.3
astropy==4.3.1
rasterio==1.2.8


Author: Sebastian Haan, Nathanial Butterworth

Copyright 2021 The University of Sydney
"""


import argparse
import os
from pathlib import Path
import numpy as np
import rasterio
import time
from astropy.convolution import convolve, Tophat2DKernel
from rasterio.warp import calculate_default_transform, reproject, Resampling

def buffer_convolve(x,buffer):
	#Make a panning window/kernel represented by 1/0 circle array
	kernel = create_buffer(buffer)
	#print(np.shape(kernel))

	#Run the convolution over the entire array with the buffer kernel
	neighbor_sum = convolve(x, kernel, boundary='extend', fill_value=0,
		normalize_kernel=False,nan_treatment='fill',preserve_nan=False)
	#Sum up the number of cells used in the kernel (to find area)
	num_neighbor = np.count_nonzero(kernel)

	#Return the density (sum/area)
	return(neighbor_sum/num_neighbor)


def buffer_convolve2(x,buffer):
	# use default uniform circle (Tophat2DKernel) rather than custom mask, need nromalization
	kernel = Tophat2DKernel(buffer) 
	neighbor_sum = convolve(x, kernel, boundary='fill', fill_value=0,
		normalize_kernel=False,nan_treatment='fill',preserve_nan=False)
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


## Arguments 
ap = argparse.ArgumentParser()
# ap.add_argument("-f","--file", default = "./data/ABS1x1km_Aus_Pop_Grid_2006_2020/data_provided/*.tif", type=Path)
# ap.add_argument("-g","--grid", default = "./data/AUS_points_5km.rds", type=Path)
# ap.add_argument("-o","--output", default = "./output", type=Path)
ap.add_argument("-f","--file", default = "./data/apg18e_1_0_0_20210512.tif", type=Path)
ap.add_argument("-g","--grid", default = "./data/grid_to_do_APMMA_NSW_20211018.tif", type=Path)
ap.add_argument("-o","--output", default = "./output", type=Path)
args = ap.parse_args()





############

if __name__ == "__main__":

	buffers = [700, 1000, 1500, 2000, 3000, 5000, 10000]

	fast_compute = False
	"""
	Resampling.nearest, 
	Resampling.bilinear,
	Resampling.cubic,
	Resampling.cubic_spline,
	Resampling.lanczos,
	Resampling.average
	"""

	fname_raster = str(args.file)
	fname_grid = str(args.grid)
	if not os.path.exists(fname_raster):
		print(f'{fname_raster} does not exist')
	if not os.path.exists(fname_grid):
		print(f'{fname_raster} does not exist')

	outpath = str(args.output)
	os.makedirs(outpath, exist_ok = True)


	t0=time.time()
	print("Reading in files...")
	gridfile = rasterio.open(fname_grid)
	grid_transform = gridfile.transform
	grid_crs=gridfile.crs
	grid_width=gridfile.width
	grid_height=gridfile.height
	grid = gridfile.read()
	grid = grid[0]
	grid_nodata = gridfile.nodata
	grid_xres, grid_yres = gridfile.res
	gridfile.close()

	datafile = rasterio.open(fname_raster)
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

	# Define temp files for convolution and reprojection
	fname_warp_temp0 = os.path.join(outpath,'data_warp_temp0.tif')
	fname_conv_temp = os.path.join(outpath,'data_conv_temp.tif')
	fname_warp_temp = os.path.join(outpath,'data_warp_temp.tif')

	buffer_min = max([data_xres, data_yres])
	print('buffer_min: ', buffer_min)

	if (data_xres > grid_xres) & (min(buffers) < 2*buffer_min) & (~fast_compute):
		# Generate first a reprojected array
		resampling_option = Resampling.lanczos
		t1_warp = time.time()
		print(f"Coordinate conversion and resampling ...")
		with rasterio.open(fname_raster) as src:
			kwargs = src.meta.copy()
			kwargs.update({'crs': grid_crs,'transform': grid_transform, 'width': grid_width,'height': grid_height})

			with rasterio.open(fname_warp_temp0, 'w', **kwargs) as dst:
				reproject(source=rasterio.band(src, 1), destination=rasterio.band(dst, 1),
					src_transform=src.transform,
					src_crs=src.crs,
					dst_transform=grid_transform,
					dst_crs=grid_crs,
					dst_width=grid_width,
					dst_height=grid_height, 
					dst_nodata=src.nodata,
					src_nodata=src.nodata,
					resampling=resampling_option)
		src.close()
		dst.close()

		datafile_proj = rasterio.open(fname_warp_temp0)
		data_proj_nodata = datafile_proj.nodata
		data_proj = datafile_proj.read()
		data_proj = data_proj[0]
		data_proj[data_proj == data_proj_nodata] = np.nan
		data_proj_xres, data_proj_yres = datafile_proj.res
		data_proj_height = datafile_proj.height
		data_proj_width = datafile_proj.width
		data_proj_crs = datafile_proj.crs
		data_proj_transform = datafile_proj.transform
		datafile_proj.close()
		t2_warp = time.time()
		print("Reprojection time: ", np.round(t2_warp-t1_warp,2))


	for buff in buffers:
		# Define output image name:
		fname_final_out = os.path.join(outpath, args.file.stem + '_buffer-' + str(buff) + 'm.tif')
		if (data_xres <= grid_xres) | (buff >= 2 * buffer_min) | fast_compute:
			# Method 1 (Convolution, then reprojection):
			resampling_option = Resampling.lanczos
			# only convolve is if buffer is large than raster resolution
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
						resampling=resampling_option)
			src.close()
			dst.close()

			t4=time.time()
			print("Coordinate Conversion time: ", np.round(t4-t3,2))

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

			# Deleting temp files:
			if os.path.exists(fname_conv_temp):
				os.remove(fname_conv_temp)
			if os.path.exists(fname_warp_temp):
				os.remove(fname_warp_temp)


			print(f"Summary Time [seconds] for buffer {buff}m")
			print("------------------------------------------")
			#print("Reading Files: ", np.round(t1-t0,2))
			print("Convolution and export: ", np.round(t3-t1,2))
			#print("Export Convolved: ", np.round(t3-t2,2))
			print("Coordinate projection: ", np.round(t4-t3,2))
			print("Masking and final Export: ", np.round(t5-t4,2))
			print("------------------------------------------")

		else:
			# Method 2 (Convolution after reprojection, typically slower):
			t2 = time.time()
			print(f"Convolving with buffer {buff} meters ...")
			# only convolve is if buffer is large than raster resolution
			b=np.ceil(buff/data_proj_xres)
			t3=time.time()
			density_array = buffer_convolve(data_proj,b)
			#print("Convolution time: ", np.round(t2-t1,2))
			print("saving convolved array...")

			with rasterio.open(fname_conv_temp,
				'w',
				driver='GTiff',
				height=data_proj_height,
				width=data_proj_width,
				count=1,
				dtype=data_dtype,
				crs=data_proj_crs,
				nodata = grid_nodata,
				transform=data_proj_transform,) as dest_file:
				dest_file.write(density_array, 1)
			dest_file.close()
			
			t4=time.time()

			# mask image
			with rasterio.open(fname_conv_temp) as src:
				data_new = src.read()[0]
				#nodata = -3.4e38
				data_new[grid == grid_nodata] = grid_nodata
				#data[data < grid_nodata] = grid_nodata
				#data[~np.isfinite(data)] = grid_nodata
				kwargs = src.meta.copy()
				kwargs.update({'nodata': grid_nodata})
				with rasterio.open(fname_final_out,'w', **kwargs) as dest_file:
					dest_file.write(data_new, 1)
			src.close()
			dest_file.close()

			t5=time.time()

			# Deleting temp files:
			if os.path.exists(fname_conv_temp):
				os.remove(fname_conv_temp)

			print(f"Summary Time [seconds] for buffer {buff}m")
			print("------------------------------------------")
			#print("Reading Files: ", np.round(t1-t0,2))
			#print("Coordinate projection: ", np.round(t2-t1,2))
			#print("Export Convolved: ", np.round(t3-t2,2))
			print("Convolution and export: ", np.round(t4-t3,2))
			print("Masking and final Export: ", np.round(t5-t4,2))
			print("------------------------------------------")

	if os.path.exists(fname_warp_temp0):
		os.remove(fname_warp_temp0)
	print("TOTAL TIME: ", np.round(t5-t0,2))
