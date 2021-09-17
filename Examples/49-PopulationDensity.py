#!/bin/python

#Assuming running from the top-level directory
import sys
sys.path.append(".")

#Import the functions
from utils import *

@delayed
def poprast_prep(pth,grid,buffs,gt0):
	#Read in current raster
	print("Running on", pth) 
	array_gdal,gt,wkt,gdal_band,nodataval = open_gdal(pth)
	print("Shape of array_gdal:",np.shape(array_gdal))

	#If the array is different to our first one must re build our trees/arrays
	if gt != gt0:
		print("Different raster extent detected.")

		#Re building Index with loop
		t1=time.time()
		print("Read",pth[-25:-20],",finding indexes...")
		ind=[]
		for row in grid.itertuples():
			ind.append(get_coords_at_point(gt, row.X, row.Y))

		grid["ind"]=ind

		print("Indexed points loop", pth[-25:-20], "Time:",np.round(time.time()-t1,2),"s")


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

	#Add additional info to the dataframe 
	yr = str(20) + re.sub(".*(apg|APG)(\\d{2}).*", "\\2", pth)
	popdf = gpd.GeoDataFrame(poplist, index = ["popdens" + str(b) for b in buffs]).T
	popdf.insert(0, 'FID', grid.FID)
	popdf.insert(0, 'year', yr)

	#Free up mem
	gdal_data=None

	return(popdf)


##### MAIN #####
if __name__ == "__main__":

	## Read in population rasters
	poprasts = glob.glob('ABS1x1km_Aus_Pop_Grid_2006_2020/data_provided/*.tif')

	#poprasts=["ABS1x1km_Aus_Pop_Grid_2006_2020/data_provided/apg06e_f_001_20210512.tif"]#,
	#			"ABS1x1km_Aus_Pop_Grid_2006_2020/data_provided/apg09e_f_001_20210512.tif"]

	buffs = [700, 1000, 1500, 2000, 3000, 5000, 10000]

	t1=time.time()
	print("Reading points rds file...")
	grid = pyreadr.read_r('AUS_points_5km.rds')
	grid=list(grid.items())[0][1]
	#grid=grid.iloc[0:100, :]

	##Shapefile
	#grid = gpd.read_file('AUS_points_1km.shp')
	#g = np.column_stack((grid.geometry.x.to_list(),grid.geometry.y.to_list()))
	#grid = pd.DataFrame(g,columns=["X","Y"])
	#grid.insert(0, 'FID', range(1, len(grid) + 1))
	
	print("Done in ", np.round(time.time()-t1,2),"s",np.shape(grid))

	## Get the raster information from the first grid
	array_gdal, gt,_,_,_ = open_gdal(poprasts[0])

	#Find the indicies of the grid in the raster assuming all rasters will have same dim
	t1=time.time()
	print("Finding indexes for grid (first time)...")
	ind=[]
	for row in grid.itertuples():
		ind.append(get_coords_at_point(gt, row.X, row.Y))

	grid["ind"]=ind
	print("Indexed points loop:", np.round(time.time()-t1,2),"s")

	#Create the dask delayed object to submit to multiple workers
	t = []
	for pth in poprasts:
		t.append(poprast_prep(pth,grid,buffs,gt))

	#Run compute with dask
	print("Finished appending, running dask compute...")
	tic=time.time()
	dd=compute(t,scheduler="multiprocessing")
	#Combine all the runs
	t=pd.concat(dd[0])
	print("Done dask:",np.round(time.time()-t1,2),"s")

	
	#Save the result to a file
	outfile="popdensity.csv"
	t.to_csv(outfile,index=False)
	print("Finished and saved output to:", outfile)
