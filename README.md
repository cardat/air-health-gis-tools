# Air Health GIS extract tools

We w

### Per single point raster querey:

Using "coregRaster" numpy points-in-circle approach: ```43.9 µs ± 2.1 µs```

Combined with numba/jit: ```21.1 µs ± 1 µs```


### Per buffer, per raster
original "1 - Population Density.py" code: ```320 s```

Using dask/swifter: ```100 s```
```
pop_area = grid.swifter.set_npartitions(16).apply(lambda x: coregRaster(x.ind[0], x.ind[1],array_gdal,7), axis=1)
```
Using dask bag:
```
bag=db.from_sequence(ind)
pop_area = bag.map(lambda x: coregRaster(x[0], x[1],array_gdal,b)).compute()
```

Using geopnadas apply: 50s per buffer, per raster
```
pop_area = grid.apply(lambda x: coregRaster(x.ind[0], x.ind[1],array_gdal,b), axis=1)
```

Using geopandas itertuples: 17.5s
```
for i,row in enumerate(sjer_plots_points.itertuples()):
		ind=get_coords_at_point(gt, row.geometry.x, row.geometry.y)
		pop[i]=coregRaster(ind[0], ind[1], array_gdal,b)
```
