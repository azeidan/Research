package org.cusp.bdi.sknn.test

import org.cusp.bdi.sknn.ds.util.SupportedSpatialIndexes
import org.cusp.bdi.sknn.SupportedKnnOperations

import org.cusp.bdi.util.{Arguments, CLArgsParser, InputFileParsers, LocalRunConsts}

object SparkKNN_Local_CLArgs {

  def apply(driverMemory: String, executorMemory: String, numExecutors: Int, executorCores: Int, firstSet: String, firstSetObj: String, secondSet: String, secondSetObj: String, k: Int, gridWidth: Int, partitionMaxByteSize: Long, knnJoinType: SupportedKnnOperations.Value, indexType: SupportedSpatialIndexes.Value): CLArgsParser =
    CLArgsParser(Arguments(debug = true, driverMemory, executorMemory, numExecutors, executorCores, firstSet, firstSetObj, secondSet, secondSetObj, k, gridWidth, partitionMaxByteSize, knnJoinType.toString, indexType.toString), Arguments.lstArgInfo())

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
      100,
      4e9.toLong,
      SupportedKnnOperations.knn,
      //      SupportedKnnOperations.allKnn,
      SupportedSpatialIndexes.quadTree)
  //      SupportedSpatialIndexes.kdTree)

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
      100,
      4e9.toLong,
      SupportedKnnOperations.knn,
      //      SupportedKnnOperations.allKnn,
      SupportedSpatialIndexes.quadTree)
  //      SupportedSpatialIndexes.kdTree)

  def bus_30_mil: CLArgsParser =
    apply("8G",
      "16G",
      4,
      5,
      LocalRunConsts.pathRandSample_A_NAD83,
      InputFileParsers.CODE_THREE_PART_LINE,
      LocalRunConsts.pathRandSample_B_NAD83,
      InputFileParsers.CODE_THREE_PART_LINE,
      10,
      100,
      2e9.toLong,
      SupportedKnnOperations.knn,
      SupportedSpatialIndexes.quadTree)
}