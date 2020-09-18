package org.cusp.bdi.fw.simba

import org.cusp.bdi.util.{Arguments, CLArgsParser, InputFileParsers, LocalRunConsts}

object Simba_Local_CLArgs {

  def apply(firstSet: String, firstSetObj: String, secondSet: String, secondSetObj: String, numPartitions: Int, k: Int) =
    CLArgsParser(Arguments(true, firstSet, firstSetObj, secondSet, secondSetObj, numPartitions, k), Arguments.lstArgInfo())

  def random_sample() =
    apply(LocalRunConsts.pathRandSample_A_NAD83,
      InputFileParsers.CODE_THREE_PART_LINE,
      LocalRunConsts.pathRandSample_B_NAD83,
      InputFileParsers.CODE_THREE_PART_LINE,
      1024,
      10)

}