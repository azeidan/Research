package org.cusp.bdi.sknn.util

import org.apache.spark.SparkContext
import org.cusp.bdi.util.InputFileParsers
import org.apache.spark.rdd.RDD
import org.cusp.bdi.util.LocalRunConsts
import org.cusp.bdi.util.LocalRunConsts

object RDD_Store {

    def getLineParser(objType: String): String => (String, (String, String)) =
        objType match {
            case s if s matches "(?i)" + LocalRunConsts.DS_CODE_TPEP_POINT => InputFileParsers.tpepPoints
            case s if s matches "(?i)" + LocalRunConsts.DS_CODE_TAXI_POINT => InputFileParsers.taxiPoints
            case s if s matches "(?i)" + LocalRunConsts.DS_CODE_THREE_PART_LINE => InputFileParsers.threePartLine
            case s if s matches "(?i)" + LocalRunConsts.DS_CODE_BUS_POINT => InputFileParsers.busPoints
            case s if s matches "(?i)" + LocalRunConsts.DS_CODE_BUS_POINT_SHIFTED => InputFileParsers.busPoints
            case s if s matches "(?i)" + LocalRunConsts.DS_CODE_RAND_POINT => InputFileParsers.randPoints
        }

    def getRDDPlain(sc: SparkContext, fileName: String, minPartitions: Int): RDD[String] =
        if (minPartitions > 0)
            sc.textFile(fileName, minPartitions)
        else
            sc.textFile(fileName)

    def getRDD(sc: SparkContext, fileName: String, objType: String): RDD[(String, (String, String))] =
        getRDD(sc, fileName, objType, 0)

    def getRDD(sc: SparkContext, fileName: String, objType: String, minPartitions: Int): RDD[(String, (String, String))] = {

        getRDDPlain(sc, fileName, minPartitions)
            .mapPartitions(_.map(getLineParser(objType)))
            .filter(_ != null)
    }
}