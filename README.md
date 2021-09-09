# Air Health GIS extract tools

Currently focusing on the slowest algorithms. 

Querying a point in a raster and getting a subsequent buffer around that point appears to be slowst routine.

## Points for embarrassing parallelisation

 * For each raster (20)
 * For each buffer (10)
 * For each point (1,000,000+)

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
