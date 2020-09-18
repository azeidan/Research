package org.cusp.bdi.sknn.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.cusp.bdi.util.InputFileParsers

object RDD_Store {

  def getRDDPlain(sc: SparkContext, fileName: String, numPartitions: Int): RDD[String] =
    if (numPartitions > 0)
      sc.textFile(fileName, numPartitions)
    else
      sc.textFile(fileName)

  def getRDD(sc: SparkContext, fileName: String, objType: String): RDD[(String, (String, String))] =
    getRDD(sc, fileName, objType, 0)

  def getRDD(sc: SparkContext, fileName: String, objType: String, numPartitions: Int): RDD[(String, (String, String))] = {

    getRDDPlain(sc, fileName, numPartitions)
      .mapPartitions(_.map(InputFileParsers.getLineParser(objType)))
      .filter(_ != null)
  }
}