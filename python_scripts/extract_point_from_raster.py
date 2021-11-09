from utils import *

#Load GIS points file (Shapes)
#Set buffers
#Use find index routines and coreraster from utils.py

#####TOOOODOOOO######
@delayed
def poprast_prep(pth,grid,buffs,gt0):
	#Read in current raster
	
	poplist = []
	for buff in buffs:
		
        
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
	poprasts = glob.glob('ABS1x1km_Aus_Pop_Grid_2006_2020/data_provided/*.tif')

	
	buffs = [700, 1000, 1500, 2000, 3000, 5000, 10000]

	t1=time.time()
	print("Reading points rds file...")
	grid = pyreadr.read_r('AUS_points_5km.rds')
	grid=list(grid.items())[0][1]
	#grid=grid.iloc[0:100, :]



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
