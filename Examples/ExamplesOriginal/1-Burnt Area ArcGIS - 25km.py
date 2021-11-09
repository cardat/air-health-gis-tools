#-------------------------------------------------------------------------------
# Name:        Burnt Area Extract
# Author:      JVB
# Created:     28/07/2021
#-------------------------------------------------------------------------------

import arcpy
import re
import os
import glob

buffer = 25
arcpy.env.overwriteOutput = True
arcpy.env.snapRaster = r'C:/Users/JoeVa/cloudstor/Shared/Bushfire_Smoke_for_CAR_Project/Predictors/data_derived_for_predicting/AUS_raster_5km.tif'
temp = 'C:/Users/JoeVa/cloudstor/Shared/Bushfire_Smoke_for_CAR_Project/Predictors/data_derived_for_predicting/Spatiotemporal/burned_area/data_derived/temp/'
Burntshps = r'C:/Users/JoeVa/cloudstor/Shared/Bushfire_Smoke_for_CAR_Project/Predictors/data_derived_for_predicting/Spatiotemporal/burned_area/data_provided/BurntArea_Aus_2000-2020.shp'
arcpy.analysis.SplitByAttributes(Burntshps, temp, "year;mm1st_b")

arcpy.env.workspace = temp
shapes = arcpy.ListFeatureClasses('*.shp')
points = r'C:/Users/JoeVa/cloudstor/Shared/Bushfire_Smoke_for_CAR_Project/Predictors/data_derived_for_predicting/AUS_points_5km.shp'
out = 'C:/Users/JoeVa/cloudstor/Shared/Bushfire_Smoke_for_CAR_Project/Predictors/data_derived_for_predicting/Spatiotemporal/burned_area/data_derived/Buffers/' + str(buffer) + 'km/'

# loop through the list of shape files for the current buffer
# This can be looped for all buffers, but will obviously take a long time
for Burntshp in shapes:
    yr = re.sub("^(\\d{4}).*", "\\1", Burntshp)
    mth = re.sub("^(\\d{4})_(\\d{1,2})_.*", "\\2", Burntshp)
    arcpy.management.CalculateField(Burntshp, 'Burnt', '0.25', field_type =  'DOUBLE')
    arcpy.conversion.PolygonToRaster(Burntshp, "Burnt", "Polytemp", "CELL_CENTER", "NONE", 500, "BUILD")
    out_raster = arcpy.sa.FocalStatistics("Polytemp", "Circle " + str(buffer) + "000 MAP", "SUM", "DATA", 90)
    out_raster.save("BurntBuff")
    arcpy.ia.ZonalStatisticsAsTable(points, "FID_1", out_raster, "Burnttable", "DATA", "SUM", "CURRENT_SLICE", 90, "AUTO_DETECT")
    arcpy.conversion.TableToTable("Burnttable", out, "Burnt" + str(buffer) + "km_" + yr + mth.zfill(2) + '.dbf', '', '', '')


for file in glob.glob(temp + "*"):
    os.remove(file)
