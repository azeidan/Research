import sys
import pyproj

proj = pyproj.Proj(init="EPSG:2263", preserve_units=True)
lon = -73.927308
lat = 40.818397
p = str(proj(lon, lat))
print p
