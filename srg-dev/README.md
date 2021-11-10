## Installation

Install the enviroment with conda (and name it as you  with, e.g. `myenv`):

```bash
conda create -n myenv python=3.9 -y
```

Install the dependencies:

```bash
conda activate myenv
conda install --file requirements.txt -y
```

## Using the Python script

TODO

With Dask
```bash
python calculate_focal_mean.py --data-path test-data/arcgis-rasters/apg18e_1_0_0_20210512.tif \
    --data-crs 3577 \
    --grid-path test-data/arcgis-rasters/grid_to_do_APMMA_NSW_20211018.tif \
    --grid-crs 9473 \
    --target-crs 3577 \
    --buffer 700 \
    --out-path test-data/test_out.tif --dask --no-write-output
```

Without Dask

```bash
python calculate_focal_mean.py --data-path test-data/arcgis-rasters/apg18e_1_0_0_20210512.tif \
    --data-crs 3577 \
    --grid-path test-data/arcgis-rasters/grid_to_do_APMMA_NSW_20211018.tif \
    --grid-crs 9473 \
    --target-crs 3577 \
    --buffer 700 \
    --out-path test-data/test_out.tif --no-write-output
```
