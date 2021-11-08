
# Previous code
| raster grid (N points) | Raster file | Nr extracted points | buffer | time | peak memory | Notes |
| - | - | - | - | - | - | - |
| ~1Mil | apg18e_APPMA_NSW.tif | 1,000 | 10,000 | 1.9s | 300 MB | dask delayed |
| ~1Mil | apg18e_APPMA_NSW.tif | 1,000 | 100,000 | 4s | 485 MB | dask delayed, seems too fast, are we sure about this? |
| ~15Mil | | 1,000 | 2,000 | 2.2s | 660 MB | dask delayed |
| ~15Mil | | 1,000 | 10,000 | 14s | 660 MB | dask delayed |
| ~15Mil | | 100,000 | 10,000 | 29s | 850 MB | dask delayed |

# Sergio code
| raster grid (N points) | Raster file | Nr extracted points | buffer | time | peak memory | Notes |
| - | - | - | - | - | - | - |
| ~1Mil | apg18e_APPMA_NSW.tif | 1,000 | 10,000 | 2.2s | 219 MB | dask dataframe multiprocess scheduler, improved extraction function, without mem profiler (1.7 sec)|
| ~1Mil | apg18e_APPMA_NSW.tif | 100,000 | 10,000 | 14min 5sec | 2600 MB | dask dataframe multiprocess scheduler, improved extraction function, (without mem profiler 5min 54 sec, 1min 4 sec to read csv)|
| ~15Mil | | 1,000 | 2,000 | 40s | 390 MB | no dask dataframe |
| ~15Mil | | 1,000 | 10,000 | 40s | 390 MB | no dask dataframe |
| ~15Mil | | 1,000 | 10,000 | 18s | 390 MB | dask dataframe multiprocess scheduler |
| ~15Mil | | 1,000 | 10,000 | 6s | 390 MB | dask dataframe multiprocess scheduler, improved extraction function |
| ~15Mil | | 100,000 | 10,000 | TBC s | TBC MB | no dask dataframe |

# ArcGIS Pro
| raster grid (N points) | Raster file | Nr extracted points | buffer | time | peak memory | Notes |
| ~100Mil | apg18e_APPMA_NSW.tif | ~100Mil | 10,000 | 81s | 5200 MB | ArcGIS Pro GUI actions added up together |
