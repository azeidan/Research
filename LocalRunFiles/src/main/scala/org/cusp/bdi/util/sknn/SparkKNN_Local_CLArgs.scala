package org.cusp.bdi.util.sknn

import org.cusp.bdi.util.CLArgsParser
import org.cusp.bdi.util.Helper
import org.cusp.bdi.util.LocalRunConsts

object SparkKNN_Local_CLArgs {

    def apply(lstArgs: List[(String, String, Any, String)], k: Int, firstSet: String, firstSetObjType: String, secondSet: String, secondSetObjType: String, minPartitions: Int) =
        CLArgsParser(StringBuilder.newBuilder
            .append("-local T")
            .append(" -debug T")
            .append(" -outDir ").append(Helper.randOutputDir("/media/cusp/Data/GeoMatch_Files/OutputFiles/"))
            .append(" -firstSet ").append(firstSet)
            .append(" -firstSetObjType ").append(firstSetObjType)
            .append(" -secondSet ").append(secondSet)
            .append(" -secondSetObjType ").append(secondSetObjType)
            .append(" -minPartitions ").append(minPartitions)
            .append(" -k ").append(k)
            .split(' '), lstArgs)

    def busPoint_busPointShift(lstArgs: List[(String, String, Any, String)]) =
        apply(lstArgs,
            10,
              LocalRunConsts.pathBus_NAD83_SMALL,
              LocalRunConsts.DS_CODE_BUS_POINT,
              LocalRunConsts.pathBus_NAD83_SMALL_SHIFTED,
              LocalRunConsts.DS_CODE_BUS_POINT_SHIFTED,
            0)

    def pathOSM_Point(lstArgs: List[(String, String, Any, String)]) =
        apply(lstArgs,
            10,
              LocalRunConsts.pathOSM_Point_WGS84,
              LocalRunConsts.DS_CODE_BUS_POINT,
              LocalRunConsts.pathOSM_Point_WGS84,
              LocalRunConsts.DS_CODE_BUS_POINT_SHIFTED,
            0)

    def busPoint_busPointShift_TINY(lstArgs: List[(String, String, Any, String)]) =
        apply(lstArgs,
            10,
              LocalRunConsts.pathBus_NAD83_TINY,
              LocalRunConsts.DS_CODE_BUS_POINT,
              LocalRunConsts.pathBus_NAD83_TINY_SHIFTED,
              LocalRunConsts.DS_CODE_BUS_POINT_SHIFTED,
            0)

    def randomPoints_randomPoints(lstArgs: List[(String, String, Any, String)]) =
        apply(lstArgs,
            10,
              LocalRunConsts.pathRandomPointsNonUniformPart1,
              LocalRunConsts.DS_CODE_RAND_POINT,
              LocalRunConsts.pathRandomPointsNonUniformPart2,
              LocalRunConsts.DS_CODE_RAND_POINT,
            17)

    def random_sample(lstArgs: List[(String, String, Any, String)]) =
        apply(lstArgs,
            10,
              LocalRunConsts.pathRandSample_A_NAD83,
              LocalRunConsts.DS_CODE_THREE_PART_LINE,
              LocalRunConsts.pathRandSample_B_NAD83,
              LocalRunConsts.DS_CODE_THREE_PART_LINE,
            17)

    def taxi_taxi_1M_No_Trip(lstArgs: List[(String, String, Any, String)]) =
        apply(lstArgs,
            10,
              LocalRunConsts.pathTaxi1M_No_Trip_NAD83_A,
              LocalRunConsts.DS_CODE_THREE_PART_LINE,
              LocalRunConsts.pathTaxi1M_No_Trip_NAD83_B,
              LocalRunConsts.DS_CODE_THREE_PART_LINE,
            17)

    def tpepPoint_tpepPoint(lstArgs: List[(String, String, Any, String)]) =
        apply(lstArgs,
            10,
              LocalRunConsts.pathTPEP_NAD83,
              LocalRunConsts.DS_CODE_RAND_POINT,
              LocalRunConsts.pathTPEP_NAD83,
              LocalRunConsts.DS_CODE_RAND_POINT,
            17)
}