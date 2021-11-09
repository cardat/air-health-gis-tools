# Python Approaches

## Previous code
| raster grid (N points) | Raster file | Nr extracted points | buffer | time | peak memory | Notes |
| - | - | - | - | - | - | - |
| ~1Mil | apg18e_APPMA_NSW.tif | 1,000 | 10,000 m | 1.9 s | 300 MB | dask delayed |
| ~1Mil | apg18e_APPMA_NSW.tif | 1,000 | 100,000 m | 4 s | 485 MB | dask delayed, seems too fast, are we sure about this? |
| ~15Mil | | 1,000 | 2,000 m | 2.2 s | 660 MB | dask delayed |
| ~15Mil | | 1,000 | 10,000 m | 14 s | 660 MB | dask delayed |
| ~15Mil | | 100,000 | 10,000 m | 29 s | 850 MB | dask delayed |

## Sergio code
| raster grid (N points) | Raster file | Nr extracted points | buffer | time | peak memory | Notes |
| - | - | - | - | - | - | - |
| ~1Mil | apg18e_APPMA_NSW.tif | 1,000 | 10,000 m | 2.2s | 219 MB | dask dataframe multiprocess scheduler, improved extraction function, without mem profiler (1.7 sec)|
| ~1Mil | apg18e_APPMA_NSW.tif | 100,000 | 10,000 m | 14min 5sec | 2600 MB | dask dataframe multiprocess scheduler, improved extraction function, (without mem profiler 5min 54 sec, 1min 4 sec to read csv)|
| ~15Mil | | 1,000 | 2,000 m | 40 s | 390 MB | no dask dataframe |
| ~15Mil | | 1,000 | 10,000 m | 40 s | 390 MB | no dask dataframe |
| ~15Mil | | 1,000 | 10,000 m | 18 s | 390 MB | dask dataframe multiprocess scheduler |
| ~15Mil | | 1,000 | 10,000 m | 6 s | 390 MB | dask dataframe multiprocess scheduler, improved extraction function |
| ~15Mil | | 100,000 | 10,000 m | 473 s | 2490 MB | no dask dataframe |
| ~15Mil | | 100,000 | 10,000 m | TBC s | TBC MB | no dask dataframe |


# Dedicated GIS Software

Both leading GIS GUI tools take about the same amount of time to do the necessary pre-processing steps of loading rasters, clipping to a matching extent, reprojecting, and resampling.

For the actual task of extracting points from a buffer, we used Focal Statistics in ArcGIS Pro and r.neighbors in QGIS.
From a usability standpoint, the ESRI implementation of convolving is more user friendly as the user can specify the size of the focal window in map units.
In QGIS, you are limited to specifying it in terms of the number of cells, and it must be an odd number.

ArcGIS Pro and QGIS appear to perform identically for 1000 meter buffer extractions, however QGIS consumes far less memory.

For buffers of 10000 meters, QGIS becomes non responsive and crashes. 
It is unclear why this happens.
ArcGIS Pro on the other hand only takes slightly less time to process the 10000 m buffer, despite the fact that there should be 100x more pixels to calculate in the buffer.

## ArcGIS Pro
| raster grid (N points) | Raster file | Nr extracted points | buffer | time | peak memory | Notes |
| - | - | - | - | - | - | - |
| ~100Mil | apg18e_APPMA_NSW.tif | ~100Mil | 1,000 m | 60 s | 5200 MB | ArcGIS Pro GUI actions added up together |
| ~100Mil | apg18e_APPMA_NSW.tif | ~100Mil | 10,000 m | 81 s | 5200 MB | ArcGIS Pro GUI actions added up together |

## QGIS
| raster grid (N points) | Raster file | Nr extracted points | buffer | time | peak memory | Notes |
| - | - | - | - | - | - | - |
| ~100Mil | apg18e_APPMA_NSW.tif | ~100Mil | 1,000 m | 60 s | 632 MB | QGIS GUI actions added up together |
| ~100Mil | apg18e_APPMA_NSW.tif | ~100Mil | 10,000 m | Crash | NA | QGIS GUI actions added up together |
