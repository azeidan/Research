package org.cusp.bdi.sknn.test

import org.cusp.bdi.util.{Arguments, CLArgsParser, InputFileParsers, LocalRunConsts}

object SparkKNN_Local_CLArgs {

  def apply(firstSet: String, firstSetObj: String, secondSet: String, secondSetObj: String, k: Int, indexType: String): CLArgsParser =
    CLArgsParser(Arguments(debug = true, firstSet, firstSetObj, secondSet, secondSetObj, k, indexType), Arguments.lstArgInfo())

  def random_sample(): CLArgsParser =
    apply(LocalRunConsts.pathRandSample_A_NAD83,
      InputFileParsers.CODE_THREE_PART_LINE,
      LocalRunConsts.pathRandSample_B_NAD83,
      InputFileParsers.CODE_THREE_PART_LINE,
      10,
      "qt")
//      "kdt")

  //  def pathOSM_Point() =
  //    apply(LocalRunConsts.pathOSM_Point_WGS84,
  //      InputFileParsers.CODE_BUS_POINT,
  //      LocalRunConsts.pathOSM_Point_WGS84,
  //      InputFileParsers.CODE_BUS_POINT_SHIFTED,
  //      0, 10)
  //
  //  def busPoint_busPointShift_TINY() =
  //    apply(
  //      LocalRunConsts.pathBus_NAD83_TINY,
  //      InputFileParsers.CODE_BUS_POINT,
  //      LocalRunConsts.pathBus_NAD83_TINY_SHIFTED,
  //      InputFileParsers.CODE_BUS_POINT_SHIFTED,
  //      0, 10)
  //
  //  def randomPoints_randomPoints() =
  //    apply(
  //      LocalRunConsts.pathRandomPointsNonUniformPart1,
  //      InputFileParsers.CODE_RAND_POINT,
  //      LocalRunConsts.pathRandomPointsNonUniformPart2,
  //      InputFileParsers.CODE_RAND_POINT,
  //      17, 10)
  //
  //
  //  def tpepPoint_tpepPoint() =
  //    apply(
  //      LocalRunConsts.pathTPEP_NAD83,
  //      InputFileParsers.CODE_RAND_POINT,
  //      LocalRunConsts.pathTPEP_NAD83,
  //      InputFileParsers.CODE_RAND_POINT,
  //      17, 10)
}