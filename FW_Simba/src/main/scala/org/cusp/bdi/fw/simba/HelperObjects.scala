package org.cusp.bdi.fw.simba

import org.cusp.bdi.util.Arguments_QueryType
import org.cusp.bdi.util.InputFileParsers
import org.cusp.bdi.util.LocalRunArgs
import org.cusp.bdi.util.LocalRunConsts

object SIM_Arguments extends Arguments_QueryType {

  override def apply() =
    super.apply()
}

object SimbaLineParser {

  def lineParser(objType: String) = objType match {
    case s if s matches "(?i)" + LocalRunConsts.DS_CODE_TPEP_POINT_WGS84 => InputFileParsers.tpepPoints_WGS84
    case s if s matches "(?i)" + LocalRunConsts.DS_CODE_OSM_POINT_WGS84 => InputFileParsers.osmPoints_WGS84
    case s if s matches "(?i)" + LocalRunConsts.DS_CODE_TPEP_POINT => InputFileParsers.tpepPoints
    case s if s matches "(?i)" + LocalRunConsts.DS_CODE_TAXI_POINT => InputFileParsers.taxiPoints
    case s if s matches "(?i)" + LocalRunConsts.DS_CODE_THREE_PART_LINE => InputFileParsers.threePartLine
    case s if s matches "(?i)" + LocalRunConsts.DS_CODE_BUS_POINT => InputFileParsers.busPoints
    case s if s matches "(?i)" + LocalRunConsts.DS_CODE_BUS_POINT_SHIFTED => InputFileParsers.busPoints
    case s if s matches "(?i)" + LocalRunConsts.DS_CODE_RAND_POINT => InputFileParsers.randPoints
  }
}

object SIM_CLArgs {

  val TPEP_Point_TPEP_Point = SIM_CLArgs("join", LocalRunConsts.pathTPEP_NAD83, "TPEP_Point_WGS84", LocalRunConsts.pathTPEP_NAD83, "TPEP_Point_WGS84")
  val OSM_Point_OSM_Point = SIM_CLArgs("join", LocalRunConsts.pathOSM_Point_WGS84, "OSM_Point_WGS84", LocalRunConsts.pathOSM_Point_WGS84, "OSM_Point_WGS84")

  def taxi_taxi_1M =
    apply("kNNJoin",
      LocalRunConsts.pathTaxi1M_NAD83,
      LocalRunConsts.DS_CODE_TAXI_POINT,
      LocalRunConsts.pathTaxi1M_NAD83,
      LocalRunConsts.DS_CODE_TAXI_POINT)

  def apply(queryType: String, firstSet: String, firstSetObj: String, secondSet: String, secondSetObj: String) = {

    val additionalParams = StringBuilder.newBuilder
      .append(" -queryType ").append(queryType)
      .append(" -matchCount 3")
      .append(" -errorRange 150")
      .append(" -matchDist 150")

    LocalRunArgs(firstSet, firstSetObj, secondSet, secondSetObj, additionalParams, SIM_Arguments())
  }

  //    def taxi_taxi_1M_No_Trip =
  //        apply("kNNJoin",
  //            LocalRunConsts.pathTaxi1M_No_Trip_NAD83_A,
  //            LocalRunConsts.DS_CODE_THREE_PART_LINE,
  //            LocalRunConsts.pathTaxi1M_No_Trip_NAD83_B,
  //            LocalRunConsts.DS_CODE_THREE_PART_LINE)

  def random_sample =
    apply("kNNJoin",
      LocalRunConsts.pathRandSample_A_NAD83,
      LocalRunConsts.DS_CODE_THREE_PART_LINE,
      LocalRunConsts.pathRandSample_B_NAD83,
      LocalRunConsts.DS_CODE_THREE_PART_LINE)

  def busPoint_busPointShift =
    apply("kNNJoin",
      LocalRunConsts.pathBus_NAD83_SMALL,
      LocalRunConsts.DS_CODE_BUS_POINT,
      LocalRunConsts.pathBus_NAD83_SMALL_SHIFTED,
      LocalRunConsts.DS_CODE_BUS_POINT_SHIFTED)

  def randomPoints_randomPoints =
    apply("kNNJoin",
      LocalRunConsts.pathRandomPointsNonUniformPart1,
      LocalRunConsts.DS_CODE_RAND_POINT,
      LocalRunConsts.pathRandomPointsNonUniformPart2,
      LocalRunConsts.DS_CODE_RAND_POINT)
}