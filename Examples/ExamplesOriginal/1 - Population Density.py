import numpy as np
import pandas as pd
import rasterio
import geopandas as gpd
import rasterstats as rs
import re
import glob

import time
## To see how long it takes


## Read in population rasters
#poprasts = glob.glob('ABS1x1km_Aus_Pop_Grid_2006_2020/' +
#                     'data_provided/*.tif')

poprasts=["ABS1x1km_Aus_Pop_Grid_2006_2020/data_provided/apg06e_f_001_20210512.tif"]#,
				#"ABS1x1km_Aus_Pop_Grid_2006_2020/data_provided/apg09e_f_001_20210512.tif"]

tic=time.time()
print("Reading points shapefile Original...")
#grid = gpd.read_file('point06.shp')
grid = gpd.read_file('AUS_points_5km.shp')
grid=grid.iloc[0:100, :]
print("Done in ", time.time()-tic)

## Add an FID in there - if one doesn't already exist
grid.insert(0, 'FID', range(1, len(grid) + 1))

## Define a 'sumna' function that removes missing, and any negative values
def sumna(x):
    return np.nansum(x[x>0])

## Function for one year's extract
def popbuff(buff):
    tic=time.time()
    b = grid.geometry.buffer(buff)
    pop = rs.zonal_stats(b,
                         poprast.read(1),
                         nodata = 0,
                         affine = poprast.transform,
                         stats = 'sum',
                         add_stats = {'sumna' : sumna})
    out = gpd.GeoDataFrame(pop)['sumna']
    print(pop,b.area)
    out = out/(b.area/1e6)
    print("Done pop ", time.time()-tic)
    return out

buffs = [700, 1000, 1500, 2000, 3000, 5000, 10000]

t = []
start = time.time()
for pth in poprasts:
    print("Running on", pth) 
    with rasterio.open(pth) as poprast:
        yr = str(20) + re.sub(".*(apg|APG)(\\d{2}).*", "\\2", pth)
        popdf = list(map(popbuff, buffs))
        popdf = gpd.GeoDataFrame(popdf, index = ["popdens" + str(b) for b in buffs]).T
        popdf.insert(0, 'FID', grid.FID)
        popdf.insert(0, 'year', yr)
        t.append(popdf)

t = pd.concat(t)
t2 = pd.DataFrame(t)

print(f'Time: {time.time() - start}')

t2.to_csv('pop6orig.csv')

### Convert to RDS in R
