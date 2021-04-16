'''
Created on Oct 26, 2017

@author: ayman
'''

import sys
import pyspark
import pyproj
from Crypto.Random.random import randint
import subprocess

# ny_state = pyproj.Proj(init="EPSG:2263", preserve_units=True)
# print ny_state(-74.269855 , 40.916046)
# print ny_state(-73.687826, 40.493325)
# print ny_state(-73.828352, 40.833273)

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print "Usage: <Bus CSV File>\n", \
              "       <Output path>\n", \
              "       <Run local (T/F)>"
        sys.exit(-1)

# proj = pyproj.Proj(init="EPSG:2263", preserve_units=True)
# print proj(-74.269855 , 40.916046)
# print proj(-73.687826, 40.493325)
# print proj(-73.828352, 40.833273)

# Parses the CSV file
# The lines are parsed by commas. Commas and newline feeds inside columns with double quotes are preserved in one column
def csvParse(part):

    join = False
    retList = []

    # A column with \n is assumed to be surronded by double quotes
    for line in part:
        for col in line.lower().split(','):

            # empty or 1 character columns
            if len(col) <= 1:
                retList.append(col)
            else:
                if join:  # True when an open quote was observed
                    if col[-1] == "\"":
                        col = col[0:-1]  # remove the last "
                        join = False
                    retList[-1] = "".join((retList[-1], ",", col))
                else:
                    if col[0:1] == "\"":  # starts with "
                        col = col[1:]  # remove the leading "
                        if col[-1] == "\"":
                            col = col[0:-1]  # remove the last "
                        else:
                            join = True
                    retList.append(col)
        if not join:
            copyList = retList[:]
            retList = []
            yield copyList

def toNAD83(part):
    
    proj = pyproj.Proj(init="EPSG:2263", preserve_units=True)
    
    for line in part:

        strOut = ""

        if(len(line) > 3):
            lat = float(line[1])
            lon = float(line[2])
             
            p = str(proj(lon, lat))
     
            xy = p.split(',')
             
            strOut = line[0] + "," + xy[0][1:] + "," + xy[1][1:-1] + "," + ",".join(line[3:])

        yield  strOut

    
sc = pyspark.SparkContext()

busCSVFile = sys.argv[1]
runLocal = sys.argv[2] == 'T'
outputDir = sys.argv[3]

if(runLocal): 
    outputDir += str(randint(0, 999))
    subprocess.call('rm -rf ' + outputDir, shell=True)

rddBikeUniqueStations = sc.textFile(busCSVFile) \
                          .mapPartitions(csvParse) \
                          .mapPartitions(lambda itr: toNAD83(itr)) \
                          .filter(lambda x: x != "") \
                          .saveAsTextFile(outputDir, "org.apache.hadoop.io.compress.GzipCodec")
if(runLocal):
    print "Output Dir: " + outputDir
