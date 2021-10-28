
# Nate code
| raster grid (N points) | Nr extracted points | buffer | time | peak memory | Notes |
| - | - | - | - | - | - |
| ~15Mil | 1,000 | 2,000 | 2.2s | 660 MB | dask delayed | 
| ~15Mil | 1,000 | 10,000 | 14s | 660 MB | dask delayed |
| ~15Mil | 100,000 | 10,000 | 29s | 850 MB | dask delayed |

# Sergio code
| raster grid (N points) | Nr extracted points | buffer | time | peak memory | Notes |
| - | - | - | - | - | - |
| ~15Mil | 1,000 | 2,000 | 40s | 390 MB | no dask dataframe |
| ~15Mil | 1,000 | 10,000 | 40s | 390 MB | no dask dataframe |
| ~15Mil | 1,000 | 10,000 | 18s | 390 MB | dask dataframe multiprocess scheduler |
| ~15Mil | 1,000 | 10,000 | 6s | 390 MB | dask dataframe multiprocess scheduler, improved extraction function |
| ~15Mil | 100,000 | 10,000 | TBC s | TBC MB | no dask dataframe |