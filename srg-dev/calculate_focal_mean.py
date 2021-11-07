import os
import logging
import argparse
import rioxarray as riox
import pandas as pd

from math import ceil
from xarray import DataArray
from rasterio.crs import CRS
from statistics import mean
from xrspatial.focal import apply, _calc_mean
from xrspatial.convolution import circle_kernel, calc_cellsize


# set up logging on screen
# TODO: move to a utils.py file
logging.basicConfig(
        format='%(asctime)s %(module)s %(levelname)s : %(message)s',
        level=logging.INFO
    )

log = logging.getLogger(__name__)


def read_raster_check_crs(raster_path: str, crs: CRS, drop_na: False) -> DataArray:
    # read data array and check CRS
    if not os.path.exists(os.path.realpath(raster_path)):
        raise FileNotFoundError(f'File not found: {raster_path}')

    raster = riox.open_rasterio(raster_path).sel(band=1)
    if raster.rio.crs is not None:
        assert raster.rio.crs == crs, (
            f"Raster CRS {raster.rio.crs.to_epsg()} not matching raster input"
            f" CRS {crs.to_epsg()}, file: {os.path.basename(raster_path)}"
        )
    else:
        log.info("setting raster CRS to passed input CRS")
        raster = raster.rio.write_crs(int(crs))

    # check for NaNs and drop them:
    if drop_na:
        nans_selection = (raster == raster.rio.nodata)
        if nans_selection.any():
            raster = raster.where(~nans_selection, drop=True)

    return raster


def main(data_path: str, data_crs: str, grid_path: str, grid_crs: str,
    target_crs: str, buffer: int, out_path: str):
    # check output path folder if exists
    out_folder = os.path.dirname(os.path.realpath(out_path))
    if not os.path.exists(out_folder):
        log.error("Folder: %s does not exists", out_folder)
        raise FileExistsError("Folder of output path does not exists!")

    # convert CRS to EPSG objects to work with rasters
    crs_list = []
    for crs in [data_crs, grid_crs, target_crs]:
        try:
            crs = CRS.from_epsg(crs)
            crs_list.append(crs)
        except Exception as e:
            log.error("Could not convert CRS: %s", crs)
            raise e
    data_crs, grid_crs, target_crs = crs_list

    # read rasters and check CRS
    data = read_raster_check_crs(data_path, data_crs)
    grid = read_raster_check_crs(grid_path, grid_crs)

    # reproject to target CRS
    # NOTE/TODO:for the grid data potentially we need the x and y arrays
    # and convert only them so we don't need to convert the raster?
    for r in [data, grid]:
        if r.rio.crs != target_crs:
            r = r.rio.reproject(target_crs)

    # extract select data at the grid
    selected_data = data.sel(x=grid.x, y=grid.y, method="nearest")

    # calculate resolution in meters and generate kernel
    pix_width, pix_height = calc_cellsize(selected_data)
    resolution = int(ceil(mean([pix_width, pix_height])))# in meters
    kernel_radius = max(int(ceil(buffer / resolution)), 1)

    kernel = circle_kernel(1, 1, kernel_radius)

    # calculate focal statistics
    output_raster = apply(selected_data, kernel, _calc_mean)

    # adjust output with scale and offset in case
    if "scale_factor" in data.attrs.keys() and data.scale_factor != 1:
        output_raster = output_raster * data.scale_factor

    if "add_offset" in data.attrs.keys() and data.add_offset != 0:
        output_raster = output_raster + data.add_offset

    # Save raster to file
    output_raster.rio.to_raster(out_path)
    pass


if __name__ == '__main__':
    time_start = pd.Timestamp('now')
    mypars = argparse.ArgumentParser(
        description="""
Process rain daily data for a given shapefile and save the output
to a location. NaNs in the data will be interpolated spatially and
they will be up/downsampled to match the passed raster."
        """,
        # formatter_class=argparse.RawTextHelpFormatter
    )
    mypars.add_argument(
        '--data-path',
        help='the path of the input data raster file',
        type=str,
        required=True
    )
    mypars.add_argument(
        '--data-crs',
        help='EPSG code of the input data CRS, ie the data are expected to be in this CRS',
        type=str,
        required=True
    )
    mypars.add_argument(
        '--grid-path',
        help='the path of the grid raster file',
        type=str,
        required=True
    )
    mypars.add_argument(
        '--grid-crs',
        help='EPSG code the input grid CRS, ie the data are expected to be in this CRS',
        type=str,
        required=True
    )
    mypars.add_argument(
        '--target-crs',
        help='EPSG code the CRS of the output data',
        type=str,
        required=True
    )
    mypars.add_argument(
        '--buffer',
        help='the size of the buffer to extract the data from, in meters',
        type=int,
        required=True
    )
    mypars.add_argument(
        '--out-path',
        help='the path of the output file to write to',
        type=str,
        required=True
    )
    argss = mypars.parse_args()
    main(**vars(argss))
    log.info("Total processing duration: %s", pd.Timestamp('now') - time_start)
