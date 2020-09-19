package org.cusp.bdi.fw.simba

import org.cusp.bdi.util.{Arguments, CLArgsParser, InputFileParsers, LocalRunConsts}

object Arguments_Simba extends Serializable {

  val local = Arguments.local
  val debug = Arguments.debug
  val outDir = Arguments.outDir
  val firstSet = Arguments.firstSet
  val firstSetObjType = Arguments.firstSetObjType
  val secondSet = Arguments.secondSet
  val secondSetObjType = Arguments.secondSetObjType
  val numPartitions = Arguments.numPartitions
  val k = Arguments.k
  val sortByEuclDist = Arguments.buildTuple("-sortByEuclDist", "Boolean", false, "Sorts the matched results by their Eucl. Dist. from the main point")

  def lstArgInfo() = List(local, debug, outDir, firstSet, firstSetObjType, secondSet, secondSetObjType, numPartitions, k, sortByEuclDist)

  def apply(debug: Boolean, firstSet: String, firstSetObj: String, secondSet: String, secondSetObj: String, numPartitions: Int, k: Int, sortByEuclDist: Boolean): Array[String] =
    Arguments(debug, firstSet, firstSetObj, secondSet, secondSetObj, numPartitions, k) ++
      Array(Arguments_Simba.sortByEuclDist._1, sortByEuclDist.toString)
}

object Simba_Local_CLArgs {

  def apply(firstSet: String, firstSetObj: String, secondSet: String, secondSetObj: String, numPartitions: Int, k: Int, sortByEuclDist: Boolean) =
    CLArgsParser(Arguments_Simba(true, firstSet, firstSetObj, secondSet, secondSetObj, numPartitions, k, sortByEuclDist), Arguments_Simba.lstArgInfo())

  def random_sample() =
    apply(LocalRunConsts.pathRandSample_A_NAD83,
      InputFileParsers.CODE_THREE_PART_LINE,
      LocalRunConsts.pathRandSample_B_NAD83,
      InputFileParsers.CODE_THREE_PART_LINE,
      1024,
      10,
      false)

}