package org.cusp.bdi.sknn.test

import org.cusp.bdi.sknn.ds.util.SupportedSpatialIndexes
import org.cusp.bdi.sknn.SupportedKnnOperations

import org.cusp.bdi.util.{Arguments, CLArgsParser, InputFileParsers, LocalRunConsts}

object SparkKNN_Local_CLArgs {

  def apply(driverMemory: String, executorMemory: String, numExecutors: Int, executorCores: Int, firstSet: String, firstSetObj: String, secondSet: String, secondSetObj: String, k: Int, knnJoinType: SupportedKnnOperations.Value, indexType: SupportedSpatialIndexes.Value): CLArgsParser =
    CLArgsParser(Arguments(debug = true, driverMemory, executorMemory, numExecutors, executorCores, firstSet, firstSetObj, secondSet, secondSetObj, k, knnJoinType.toString, indexType.toString), Arguments.lstArgInfo())

  def random_sample(): CLArgsParser =
    apply("2G",
      "4G",
      4,
      5,
      LocalRunConsts.pathRandSample_A_NAD83,
      InputFileParsers.CODE_THREE_PART_LINE,
      LocalRunConsts.pathRandSample_B_NAD83,
      InputFileParsers.CODE_THREE_PART_LINE,
      10,
      //      SupportedKnnOperations.knn,
      SupportedKnnOperations.allKnn,
                  SupportedSpatialIndexes.quadTree)
//      SupportedSpatialIndexes.kdTree)
}