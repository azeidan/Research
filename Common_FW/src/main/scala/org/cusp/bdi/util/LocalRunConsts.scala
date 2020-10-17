package org.cusp.bdi.util

import java.io.FileWriter

object LocalRunConsts {

  val pathOutput = "/media/cusp/Data/GeoMatch_Files/OutputFiles/"
  val sparkWorkDir = "/media/cusp/Data/GeoMatch_Files/spark_work_dir/"
  val localRunLogFile = "/media/ayman/Data/GeoMatch_Files/OutputFiles/batchTestResults.txt"
  val benchmarkLogFile = "/media/ayman/Data/GeoMatch_Files/OutputFiles/benchmarkResults.txt"

  val pathLION_NAD83 = "/media/cusp/Data/GeoMatch_Files/InputFiles/LION_NYC_Streets_NAD83_Enumerated_WKT.csv"
  val pathLION_WGS84 = "/media/cusp/Data/GeoMatch_Files/InputFiles/LION_NYC_Streets_Lat_Lon_Enumerated_WKT.csv"
  val pathTPEP_NAD83 = "/media/cusp/Data/GeoMatch_Files/InputFiles/Trip_Record_TPEP_NAD83.csv" // "/media/cusp/Data/GeoMatch_Files/InputFiles/TBD.csv"
  val pathTPEP_WGS84 = "/media/cusp/Data/GeoMatch_Files/InputFiles/Trip_Record_TPEP_WGS84.csv"
  val pathTaxi_NAD83 = "/media/cusp/Data/GeoMatch_Files/InputFiles/Trip_Record_Taxi_NAD83.csv"
  val pathTaxi_WGS84 = "/media/cusp/Data/GeoMatch_Files/InputFiles/Trip_Record_Taxi_WGS84.csv"
  val pathBus_NAD83 = "/media/cusp/Data/GeoMatch_Files/InputFiles/Trip_Record_Bus_NAD83.csv"
  val pathBus_NAD83_SMALL = "/media/cusp/Data/GeoMatch_Files/InputFiles/Bus_TripRecod_NAD83_part-00000_SMALL.csv"
  val pathBus_NAD83_TINY = "/media/cusp/Data/GeoMatch_Files/InputFiles/Bus_TripRecod_NAD83_part-00000_TINY.csv"
  val pathBus_NAD83_SMALL_SHIFTED = "/media/cusp/Data/GeoMatch_Files/InputFiles/Bus_TripRecod_NAD83_part-00000_SMALL_shifted(100-300).csv"
  val pathBus_NAD83_TINY_SHIFTED = "/media/cusp/Data/GeoMatch_Files/InputFiles/Bus_TripRecod_NAD83_part-00000_TINY_shifted(100-300).csv"
  val pathBus_WGS84 = "/media/cusp/Data/GeoMatch_Files/InputFiles/Trip_Record_Bus_WGS84.csv"
  val pathRandomPointsNonUniformPart1 = "/media/cusp/Data/GeoMatch_Files/InputFiles/randomPoints_500K_nonUniform_part1.csv"
  //    val pathRandomPointsNonUniformPart2 = "/media/cusp/Data/GeoMatch_Files/InputFiles/randomPoints_500K_nonUniform_part2.csv"
  val pathRandomPointsNonUniformPart2 = "/media/cusp/Data/GeoMatch_Files/InputFiles/randomPoints_1M_nonUniform_part2.csv"

  val pathOSM_Point_WGS84 = "/media/cusp/Data/GeoMatch_Files/InputFiles/Trip_Record_OSM_Points.csv"

  val pathKM_Bus_SMALL = "/media/cusp/Data/GeoMatch_Files/InputFiles/Bus_TripRecod_NAD83_part-00000_SMALL_key_SORTED_k30.csv"
  val pathKM_TPEP = "/media/cusp/Data/GeoMatch_Files/InputFiles/KM_LION_LineString_TPEP_Point_ErrCorr_MBRExp_150_MaxDist_150.1K.csv"
  val pathKM_TPEP_WGS84 = "/media/cusp/Data/GeoMatch_Files/InputFiles/KM_WGS84_LION_LineString_TPEP_Point_ErrCorr_MBRExp_150_MaxDist_150.1K.csv"
  val pathKM_Taxi = "/media/cusp/Data/GeoMatch_Files/InputFiles/KM_LION_LineString_Taxi_Point_ErrCorr_MBRExp_150_MaxDist_1503.1K.csv"
  val pathKM_Taxi_WGS84 = "/media/cusp/Data/GeoMatch_Files/InputFiles/KM_WGS84_LION_LineString_Taxi_Point_ErrCorr_MBRExp_150_MaxDist_150.1K.csv"
  val pathKM_Bus = "/media/cusp/Data/GeoMatch_Files/InputFiles/KM_LION_LineString_Bus_Point_ErrCorr_MBRExp_150_MaxDist_150.1K.csv"
  val pathKM_Bus_WGS84 = "/media/cusp/Data/GeoMatch_Files/InputFiles/KM_WGS84_LION_LineString_Bus_Point_ErrCorr_MBRExp_150_MaxDist_150.1K.csv"
  val pathKM_RandomPointsNonUniform = ""

  val pathGM_TPEP = "/media/cusp/Data/GeoMatch_Files/OutputFiles/Simba/Bread_3_Grp2/"
  val pathSparkKNN_FW_Output_1: String = pathGM_TPEP
  val pathSparkKNN_FW_Output_2 = "/media/cusp/Data/GeoMatch_Files/OutputFiles/807/"

  val pathGS_TPEP = "/media/cusp/Data/GeoMatch_Files/InputFiles/GeoSpark_LION_TPEP.csv"
  val pathLS_wgs_TPEP = "/media/cusp/Data/GeoMatch_Files/InputFiles/LocationSpark_LION_TPEP.csv"
  val pathLS_wgs_Taxi = "/media/cusp/Data/GeoMatch_Files/InputFiles/LocationSpark_LION_Taxi.csv"
  val pathLS_wgs_Bus = "/media/cusp/Data/GeoMatch_Files/InputFiles/LocationSpark_LION_Bus.csv"
  val pathGM_Taxi = "/media/cusp/Data/GeoMatch_Files/InputFiles/GeoMatch_LION_Taxi.csv"
  val pathGS_Taxi = "/media/cusp/Data/GeoMatch_Files/InputFiles/GeoSpark_LION_Taxi.csv"

  val pathTaxi1M_NAD83 = "/media/cusp/Data/GeoMatch_Files/InputFiles/Yellow_TLC_TripRecord_NAD83_1M.csv"

  val pathRandSample_A_NAD83 = "/media/cusp/Data/GeoMatch_Files/InputFiles/RandomSamples/Bread_3_A.csv"
  val pathRandSample_B_NAD83 = "/media/cusp/Data/GeoMatch_Files/InputFiles/RandomSamples/Bread_3_B.csv"


  def logLocalRunEntry(fileName: String, label: String, file1: String, file2: String, outDir: String, time: Double): Unit = {

    val fw = new FileWriter(fileName, true)

    fw.write("%s\t%s\t%s\t%s\t%.4f\t%n".format(label, file1, file2, outDir, time))

    fw.flush()
    fw.close()
  }
}