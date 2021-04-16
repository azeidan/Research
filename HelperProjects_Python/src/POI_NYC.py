'''
Created on Oct 26, 2017

@author: ayman
'''

import sys
# import pyspark
import pyproj
# from Crypto.Random.random import randint
import subprocess

# ny_state = pyproj.Proj(init="EPSG:2263", preserve_units=True)
# print ny_state(-74.269855 , 40.916046)
# print ny_state(-73.687826, 40.493325)
# print ny_state(-73.828352, 40.833273)

# proj = pyproj.Proj(init="EPSG:2263", preserve_units=True)
# print proj(-74.269855 , 40.916046)
# print proj(-73.687826, 40.493325)
# print proj(-73.828352, 40.833273)

# def toNAD83(part):
#
#     proj = pyproj.Proj(init="EPSG:2263", preserve_units=True)
#
#     for line in part:
#
#         strOut = ""
#
#         if(len(line) > 3):
#             lat = float(line[1])
#             lon = float(line[2])
#
#             p = str(proj(lon, lat))
#
#             xy = p.split(',')
#
#             strOut = line[0] + "," + xy[0][1:] + "," + xy[1][1:-1] + "," + ",".join(line[3:])
#
#         yield  strOut

fr = open("/media/ayman/Data/GeoMatch_Files/InputFiles/core_poi_NYC_(Long_Lat).csv", "r")
fw = open("/media/ayman/Data/GeoMatch_Files/InputFiles/core_poi_NYC_(NAD83).csv", "w")

# proj = pyproj.Proj(init="EPSG:2263", preserve_units=True)
proj = pyproj.Proj("EPSG:2263", True)

# print proj(-74.00849, 40.618913)
#
# header
arr = fr.next().split(',')
fw.write(arr[0] + "," + arr[1] + "," + arr[2] + "," + arr[3] + "," + arr[4] + "," + arr[5] + "," + arr[6] + "," +
         arr[7] + "," + arr[8] + "," + arr[9] + "," + "X" + "," + "Y" + "," + arr[12] + "," +
         arr[13] + "," + arr[14] + "," + arr[15] + "," + arr[16] + "," + arr[17] + "," + arr[18] + "," + arr[19] +
         "," + arr[20] + "," + arr[21] + "," + arr[22] + "," + arr[23] + "\n")

for x in fr:
    arr = x.split(',')

    nad = proj(float(arr[11]), float(arr[10]))

    fw.write(arr[0] + "," + arr[1] + "," + arr[2] + "," + arr[3] + "," + arr[4] + "," + arr[5] + "," + arr[6] + "," +
             arr[7] + "," + arr[8] + "," + arr[9] + "," + str(nad[0]) + "," + str(nad[1]) + "," + arr[12] + "," +
             arr[13] + "," + arr[14] + "," + arr[15] + "," + arr[16] + "," + arr[17] + "," + arr[18] + "," + arr[19] +
             "," + arr[20] + "," + arr[21] + "," + arr[22] + "," + arr[23] + "\n")
