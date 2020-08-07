package org.cusp.bdi.sb.fw.ls

import org.cusp.bdi.util.{Arguments_QueryType, CLArgsParser, LocalRunArgs, LocalRunConsts}

object LS_Arguments extends Arguments_QueryType {

  val sIdx = ("sIdx", "Char", null, "Spatial Index Type (G=Grid, I=IRTree, Q=QuadTree, R=RTree)")

  override def apply() =
    super.apply() ++ List(sIdx)
}

object LS_CLArgs {

  val lion_Box_TPEP_Point = LS_CLArgs("rangeJoin", LocalRunConsts.pathLION_WGS84, "LION_wgs_Box", LocalRunConsts.pathTPEP_WGS84, "TPEP_wgs_Point")
  val lion_Box_Taxi_Point = LS_CLArgs("rangeJoin", LocalRunConsts.pathLION_WGS84, "LION_wgs_Box", LocalRunConsts.pathTaxi_WGS84, "Taxi_wgs_Point")
  val lion_Box_Bus_Point = LS_CLArgs("rangeJoin", LocalRunConsts.pathLION_WGS84, "LION_wgs_Box", LocalRunConsts.pathBus_WGS84, "Bus_wgs_Point")

  private def getCLArgsParser(args: String) =
    CLArgsParser(args.split(' '), LS_Arguments())

  private def apply(queryType: String, firstSet: String, firstSetObj: String, secondSet: String, secondSetObj: String) = {

    val additionalParams = StringBuilder.newBuilder
      .append(" -queryType ").append(queryType)
      .append(" -matchCount 3")
      .append(" -errorRange 0.00226") // adding 150 to longitude and latitude translates to Euclidean distance 0.00033 and 0.00046 respectively
      .append(" -matchDist 0.00226") // 0.0005 covers both distances (0.00033 and 0.00046)
      .append(" -sIdx R")

    LocalRunArgs(firstSet, firstSetObj, secondSet, secondSetObj, additionalParams, LS_Arguments())
  }
}