from utils import *
import argparse
from pathlib import Path

"""Constants and Environment"""

ap = argparse.ArgumentParser()
ap.add_argument("-f","--file", default = "./data/ABS1x1km_Aus_Pop_Grid_2006_2020/data_provided/apg06e_f_001_20210512.tif", type=Path)
ap.add_argument("-g","--grid", default = "./data/AUS_points_5km.rds", type=Path)
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

	buffs = [700, 1000, 1500, 2000, 3000, 5000, 10000]

	t1=time.time()
	print("Reading points rds file...")
	grid = pyreadr.read_r(str(args.grid))
	grid=list(grid.items())[0][1]
	#grid=grid.iloc[0:100, :]

	##Shapefile
	#grid = gpd.read_file('AUS_points_1km.shp')
	#g = np.column_stack((grid.geometry.x.to_list(),grid.geometry.y.to_list()))
	#grid = pd.DataFrame(g,columns=["X","Y"])
	#grid.insert(0, 'FID', range(1, len(grid) + 1))


	#For tree
	#g = np.column_stack((grid.geometry.x.to_list(),grid.geometry.y.to_list()))
	#print("Stacked points in",time.time()-tic)
	
	print("Done in ", np.round(time.time()-t1,2),"s",np.shape(grid))

	print(poprasts[0])

	## Get the raster information from the first grid
	test = open_gdal(poprasts[0])
	print("hi ",test)

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
		#dd=poprast_prep(pth,grid,buffs,gt)

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
