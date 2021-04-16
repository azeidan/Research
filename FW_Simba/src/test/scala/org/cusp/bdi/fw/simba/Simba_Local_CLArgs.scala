package org.cusp.bdi.fw.simba

import org.cusp.bdi.util.{Arguments, CLArgsParser, InputFileParsers, LocalRunConsts}

//object Arguments_Simba extends Serializable {
//
//  val sortByEuclDist: (String, String, Boolean, String) = Arguments.buildTuple("-sortByEuclDist", "Boolean", false, "Sorts the matched results by their Eucl. Dist. from the main point")
//
//  def lstArgInfo(): List[(String, String, Any, String)] = Arguments.lstArgInfo() ++ List(sortByEuclDist)
//
//  def apply(firstSet: String, firstSetObj: String, secondSet: String, secondSetObj: String, k: Int, sortByEuclDist: Boolean): Array[String] =
//    Arguments(true,
//      "2G",
//      "4G",
//      4,
//      5,
//      firstSet,
//      firstSetObj,
//      secondSet,
//      secondSetObj,
//      k,
//      "",
//      "",
//      (Arguments_Simba.sortByEuclDist._1, sortByEuclDist.toString))
//}

object Simba_Local_CLArgs {

  object SupportedKnnOperations extends Enumeration with Serializable {

    val knn: SupportedKnnOperations.Value = Value("knn")
    val allKnn: SupportedKnnOperations.Value = Value("allknn")
  }

  def apply(driverMemory: String, executorMemory: String, numExecutors: Int, executorCores: Int, firstSet: String, firstSetObj: String, secondSet: String, secondSetObj: String, k: Int, knnJoinType: SupportedKnnOperations.Value): CLArgsParser =
    CLArgsParser(Arguments(debug = true, driverMemory, executorMemory, numExecutors, executorCores, firstSet, firstSetObj, secondSet, secondSetObj, k, -1, -1, knnJoinType.toString, ""), Arguments.lstArgInfo())

  def corePOI_NYC =
    apply("2G",
      "8G",
      4,
      5,
      LocalRunConsts.pathPOI_NYC_NAD83,
      InputFileParsers.CODE_POI_NYC,
      LocalRunConsts.pathRandSample_B_NAD83,
      InputFileParsers.CODE_THREE_PART_LINE,
      10,
      SupportedKnnOperations.knn)

  def random_sample: CLArgsParser =
    apply("2G",
      "8G",
      4,
      5,
      LocalRunConsts.pathRandSample_A_NAD83,
      InputFileParsers.CODE_THREE_PART_LINE,
      LocalRunConsts.pathRandSample_B_NAD83,
      InputFileParsers.CODE_THREE_PART_LINE,
      10,
      SupportedKnnOperations.knn)
}