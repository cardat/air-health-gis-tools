# Air Health GIS extract tools

## Installation

1. Setup your python environment:

First, we will want to create and activate a Python virtual environment.
```
pyenv virtualenv 3.6.9 air-health-gis

pyenv local air-health-gis 
```

Second, we will install the required packages into our virtual environment.
```
pip install -r requirements.txt
```


## Points for embarrassing parallelisation

 * For each raster (20)
 * For each buffer (10)
 * For each point (1,000,000+)
 
## Considerations

The radius of the buffer may intersect a raster cell. In this case the raster cell is included within the buffer calc if it less than  ceil(buffer/cell size)

Ivan has suggested converting all underlying grids/shapefiles/polygons to a single file format. This could be a good way forward as the routine for for extracting from a raster is pretty good now.

The 5 generic types of GIS extractions are all listed as "extract_lblahblah.py". These will be templates to make a script for each of the layers listed in the xlsx. I think each layer will need to be approached uniquely though, as small things like band names, file-naming conventions, will all be different (bringing us back to the approach sugested by Ivan to homogenise the inputs). So far only, 1 layer is finished in the Examples folder.

**utils.py** contains all the helper functions, that may need more generalising as each of the 5 extraction methods are implemented.

## extract_points_from_raster_buffer.py - done
Currently focusing on the slowest algorithms. 

Querying a point in a raster and getting a subsequent buffer around that point appears to be slowst routine.


### Per single point raster querey:

Original "1 - Population Density.py" code, zonalstats: ```0.1 s``` per point.

Using "coregRaster" numpy points-in-circle approach: ```43.9 µs ± 2.1 µs```per point.
This algorithm has about a 1% difference to that returned by zonalstats.

Combined with numba/jit: ```21.1 µs ± 1 µs``` per point.

Using a square buffer region instead of a circle increase the speed to: ```2.79 µs ± 3.64 µs```
But this coincides with an difference in reported results of around 10% compared with zonalstats.


### Per buffer, per raster
Original "1 - Population Density.py" code, "zonal stats": ```320 s``` per buffer, per raster

Using dask/swifter apply: ```100 s```
```
pop_area = grid.swifter.set_npartitions(16).apply(lambda x: coregRaster(x.ind[0], x.ind[1],array_gdal,b), axis=1)
```

Using dask bag: ```100 s```
```
bag=db.from_sequence(ind)
pop_area = bag.map(lambda x: coregRaster(x[0], x[1],array_gdal,b)).compute()
```

Using geopnadas apply: ```50 s``` per buffer, per raster
```
pop_area = grid.apply(lambda x: coregRaster(x.ind[0], x.ind[1],array_gdal,b), axis=1)
```

Using geopandas itertuples: ```17.5 s``` per buffer, per raster
```
for i,row in enumerate(sjer_plots_points.itertuples()):
		ind=get_coords_at_point(gt, row.geometry.x, row.geometry.y)
		pop[i]=coregRaster(ind[0], ind[1], array_gdal,b)
```

1380 s for all rasters and all buffers!

Using convolution of the raste with a buffer-kernel combined with the itertuples is the fastest appraoch and scales to at least a 1-km resolution grid-as implemented.

### Tried and failed
Using scipy kd tree to build index of the raster points. Then would query the points of interest. Any raster larger than 25,000,000 (5000x5000 array) points takes over 10 minutes to build an index. 


## extract_count_points_in_buffer.py - todo

## extract_lines_in_buffer.py - todo

## extract_point_from_raster.py - todo

## extract_polygon_in_buffer.py - todo



