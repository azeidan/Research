package org.cusp.bdi.fw.simba

import org.cusp.bdi.util.{Arguments, CLArgsParser, InputFileParsers, LocalRunConsts}

object Arguments_Simba extends Serializable {

  val sortByEuclDist: (String, String, Boolean, String) = Arguments.buildTuple("-sortByEuclDist", "Boolean", false, "Sorts the matched results by their Eucl. Dist. from the main point")

  def lstArgInfo(): List[(String, String, Any, String)] = Arguments.lstArgInfo() ++ List(sortByEuclDist)

  def apply(firstSet: String, firstSetObj: String, secondSet: String, secondSetObj: String, k: Int, sortByEuclDist: Boolean): Array[String] =
    Arguments(true,
      "2G",
      "4G",
      4,
      5,
      firstSet,
      firstSetObj,
      secondSet,
      secondSetObj,
      k,
      "",
      "",
      (Arguments_Simba.sortByEuclDist._1, sortByEuclDist.toString))
}

object Simba_Local_CLArgs {

  def apply(firstSet: String, firstSetObj: String, secondSet: String, secondSetObj: String, k: Int, sortByEuclDist: Boolean): CLArgsParser =
    CLArgsParser(Arguments_Simba(firstSet, firstSetObj, secondSet, secondSetObj, k, sortByEuclDist = sortByEuclDist), Arguments_Simba.lstArgInfo())

  def random_sample(): CLArgsParser =
    apply(LocalRunConsts.pathRandSample_A_NAD83,
      InputFileParsers.CODE_THREE_PART_LINE,
      LocalRunConsts.pathRandSample_B_NAD83,
      InputFileParsers.CODE_THREE_PART_LINE,
      10,
      sortByEuclDist = true)

}