import utils
import argparse
from pathlib import Path
from osgeo import gdal

ap = argparse.ArgumentParser()
ap.add_argument("-f","--file", default = "./data/ABS1x1km_Aus_Pop_Grid_2006_2020/data_provided/apg06e_f_001_20210512.tif", type=Path)
args = ap.parse_args()

gdal.UseExceptions()

if __name__ == "__main__":

    print("Let's go hunting seg faults!")

    print("Does ", str(args.file), " cause a seg fault?")
    test = utils.open_gdal(str(args.file))

    print(test, " doesn't cause a seg fault! yay :)")

