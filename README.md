# Air Health GIS extract tools

## Table of Contents
  * [Introduction](#introduction)
  * [Installation](#installation)
  * [Technical Requirements](#technical-requirements)
  * [User Guide](#user-guide)
  * [Acknowledgement](#acknowledgement)
  * [Old Readme](#old-readme)

## Introduction

This repository contains code for replicating an ArcGIS Pro GIS data exraction workflow in Python, for use by researchers at the Centre for Air pollution, energy and health Research Data Analysis Technology (CARDAT).

The workflow involves:
1. Loading in a grid raster and data raster layer.
2. Cropping the data raster to the same extent as the grid raster.
3. Reprojecting and resampling the data raster to the same projection and resolution as the grid raster.
4. Calculating mean values for the data raster within buffers around each pixel in the grid raster.
5. Saving results as rasters.

## Installation

1. Setup your python environment with `conda` (if you don't have miniconda/anaconda please download it [here](https://www.anaconda.com/products/individual)):

__NOTE__: make sure you have your "base" conda enviroment updated (`conda update -n base -c defaults conda -y`), and also __pay attention__ to never install packages into the base environment!

Add or make sure to have the `conda-forge` channel as part of your conda distribution:

```bash
conda config --append channels conda-forge
```

First, we will want to create and activate a Python virtual environment.

```bash
conda create -n air-health-gis python=3.9
```

Second, we activate our virtual enviroment and install the required packages.

```bash
conda activate air-health-gis
conda install --file conda-requirements.txt -y
pip install pip-requirements.txt
```

## Technical Requirements

For development, extractions were tested locally on a macOS machine with the following specs:

- Operating System: macOS Big Sur
- Model: MacBook Pro 16-inch 2019
- Processor: 2.3 GHz 8-Core Intel Core i9
- Memory: 64 GB 2667 MHz DDR4 Ram

The performance figures quoted in this Readme will be measured on a machine with those specs; expect potential decreases in performance with reduced processor and memory power.

## User Guide

### Example Scenario

For demonstrating how to use these python scripts, and comparing against performance with industry standard GUI tools like ArcGIS Pro, we will use the example use case.

In this scenario, we have a grid raster file that has 113m pixels the cover the extent of New South Wales. 
This grid has approximately 100 million pixels.

We want to extract information from a data raster layer of population density, from the Australian Bureau of Statistics. This raster is natively at 1km resolution.

### Basic usage

The extraction script can easily be controlled using a command line interface:

```bash
python extract_raster_buffer.py -d <path/filename of data raster> -g <path/filename of grid raster>  -b <list of one or more buffers> -o <output folder>
```

Using our example scenario, if we want to extract for 700m, 1km and 10km buffers, we would run:
```bash
python extract_raster_buffer.py -d ./data/apg18e_1_0_0_20210512.tif -g ./data/grid_to_do_APMMA_NSW_20211018.tif -b 700 1000 10000
```

### Benchmarking



#### Dedicated GIS Software

##### ArcGIS Pro
| raster grid (N points) | Raster file | Nr extracted points | buffer | time | peak memory | Notes |
| - | - | - | - | - | - | - |
| ~100Mil | apg18e_APPMA_NSW.tif | ~100Mil | 1,000 m | 60 s | 5200 MB | ArcGIS Pro GUI actions added up together |
| ~100Mil | apg18e_APPMA_NSW.tif | ~100Mil | 10,000 m | 81 s | 5200 MB | ArcGIS Pro GUI actions added up together |
##### QGIS
| raster grid (N points) | Raster file | Nr extracted points | buffer | time | peak memory | Notes |
| - | - | - | - | - | - | - |
| ~100Mil | apg18e_APPMA_NSW.tif | ~100Mil | 1,000 m | 60 s | 632 MB | QGIS GUI actions added up together |
| ~100Mil | apg18e_APPMA_NSW.tif | ~100Mil | 10,000 m | Crash | NA | QGIS GUI actions added up together |

Both leading GIS GUI tools take about the same amount of time to do the necessary pre-processing steps of loading rasters, clipping to a matching extent, reprojecting, and resampling.

For the actual task of extracting points from a buffer, we used Focal Statistics in ArcGIS Pro and r.neighbors in QGIS.
From a usability standpoint, the ESRI implementation of convolving is more user friendly as the user can specify the size of the focal window in map units (e.g. meters).
In QGIS, you are limited to specifying it in terms of the number of cells, and it must be an odd number.

ArcGIS Pro and QGIS appear to perform identically for 1000 meter buffer extractions, however QGIS consumes far less memory.

For buffers of 10000 meters, QGIS becomes non responsive and crashes.
It is unclear why this happens.
ArcGIS Pro on the other hand only takes slightly less time to process the 10000 m buffer, despite the fact that there should be 100x more pixels to calculate in the buffer.


## Acknowledgement

The use of the SIH services including the Artemis HPC and associated support and training warrants acknowledgement in any publications, conference proceedings or posters describing work facilitated by these services.

The continued acknowledgment of the use of SIH facilities ensures the sustainability of our services.

Please seek our approval and send us a submission draft before including SIH staff as authors in any publications.

> The authors acknowledge the technical assistance provided by the Sydney Informatics Hub, a Core Research Facility of the University of Sydney.

Acknowledging specific staff:

> The authors acknowledge the technical assistance of (name of staff) of the Sydney Informatics Hub, a Core Research Facility of the University of Sydney.

For further information about acknowledging the Sydney Informatics Hub, please contact us at sih.info@sydney.edu.au.

## Old Readme
 
### Considerations

The radius of the buffer may intersect a raster cell. In this case the raster cell is included within the buffer calc if it less than  ceil(buffer/cell size)

Ivan has suggested converting all underlying grids/shapefiles/polygons to a single file format. This could be a good way forward as the routine for for extracting from a raster is pretty good now.

The 5 generic types of GIS extractions are all listed as "extract_<geospatial_feature>.py". These will be templates to make a script for each of the layers listed in the xlsx. I think each layer will need to be approached uniquely though, as small things like band names, file-naming conventions, will all be different (bringing us back to the approach sugested by Ivan to homogenise the inputs). So far only, 1 layer is finished in the Examples folder.
