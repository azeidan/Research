package org.cusp.bdi.util

import java.io.FileWriter
import scala.collection.mutable.ListBuffer

object LocalRunConsts {

  val spacer: String = "%120s%n".format("").replaceAll(" ", "=")

  val pathOutput = "/media/cusp/Data/GeoMatch_Files/OutputFiles/"
  val sparkWorkDir = "/media/cusp/Data/GeoMatch_Files/spark_work_dir"
  val localRunLogFile = "/media/ayman/Data/GeoMatch_Files/OutputFiles/batchTestResults.txt"
  val localRunDebugLogFile = "/media/ayman/Data/GeoMatch_Files/OutputFiles/batchTestResultsDebugInfo.txt"
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

  //  val pathGM_TPEP = "/media/cusp/Data/GeoMatch_Files/OutputFiles/Simba/Bread_3_Grp1/*.gz"
  //  val pathGM_TPEP = "/media/cusp/Data/GeoMatch_Files/OutputFiles/Simba/ALL_Knn/Taxi_1_Grp1/*.gz"
  val pathGM_TPEP = "/media/cusp/Data/GeoMatch_Files/OutputFiles/Simba/Bread_1_Grp1/*.gz"
  val pathSparkKNN_FW_Output_1: String = pathGM_TPEP
  val pathSparkKNN_FW_Output_2 = "/media/cusp/Data/GeoMatch_Files/OutputFiles/199/*.gz"

  val pathGS_TPEP = "/media/cusp/Data/GeoMatch_Files/InputFiles/GeoSpark_LION_TPEP.csv"
  val pathLS_wgs_TPEP = "/media/cusp/Data/GeoMatch_Files/InputFiles/LocationSpark_LION_TPEP.csv"
  val pathLS_wgs_Taxi = "/media/cusp/Data/GeoMatch_Files/InputFiles/LocationSpark_LION_Taxi.csv"
  val pathLS_wgs_Bus = "/media/cusp/Data/GeoMatch_Files/InputFiles/LocationSpark_LION_Bus.csv"
  val pathGM_Taxi = "/media/cusp/Data/GeoMatch_Files/InputFiles/GeoMatch_LION_Taxi.csv"
  val pathGS_Taxi = "/media/cusp/Data/GeoMatch_Files/InputFiles/GeoSpark_LION_Taxi.csv"

  val pathTaxi1M_NAD83 = "/media/cusp/Data/GeoMatch_Files/InputFiles/Yellow_TLC_TripRecord_NAD83_1M.csv"

//  val pathRandSample_A_NAD83 = "/media/cusp/Data/GeoMatch_Files/InputFiles/RandomSamples/0_Bus_All.csv"
    val pathRandSample_A_NAD83 = "/media/cusp/Data/GeoMatch_Files/InputFiles/RandomSamples_OLD/Bread_1_A.csv"
//  val pathRandSample_B_NAD83 = "/media/cusp/Data/GeoMatch_Files/InputFiles/RandomSamples/0_Bus_All.csv"
      val pathRandSample_B_NAD83 = "/media/cusp/Data/GeoMatch_Files/InputFiles/RandomSamples_OLD/Bread_1_B.csv"

  def logLocalRunEntry(logFileName: String, message: String, debugInfoFileName: String, lstDebugInfo: ListBuffer[String]): Unit = {

    val fw = new FileWriter(logFileName, true)

    fw.write(message)

    fw.flush()
    fw.close()

    if (debugInfoFileName != null) {

      val fw = new FileWriter(debugInfoFileName, true)

      fw.write(spacer)
      fw.write(message)
      fw.write(spacer)

      lstDebugInfo.foreach(message => fw.write(message + "\n"))

      fw.write(spacer)
      fw.write(spacer)
      fw.write("\n\n\n")

      fw.flush()
      fw.close()
    }
  }
}