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
}