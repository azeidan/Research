package org.cusp.bdi.sknn.test

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.cusp.bdi.ds.geom.Point
import org.cusp.bdi.sknn.ds.util.SupportedSpatialIndexes
import org.cusp.bdi.sknn.{SparkKnn, SupportedKnnOperations}
import org.cusp.bdi.util._

object TestAllKnnJoin {

  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()
    //    var startTime2 = startTime

    //    val clArgs = SparkKNN_Local_CLArgs.bus_30_mil
//    val clArgs = SparkKNN_Local_CLArgs.random_sample
    val clArgs = SparkKNN_Local_CLArgs.corePOI_NYC
//    val clArgs = CLArgsParser(args, Arguments.lstArgInfo())

    //    val clArgs = SparkKNN_Local_CLArgs.busPoint_busPointShift(Arguments())
    //    val clArgs = SparkKNN_Local_CLArgs.busPoint_taxiPoint(Arguments())
    //    val clArgs = SparkKNN_Local_CLArgs.tpepPoint_tpepPoint(Arguments())

    val localMode = clArgs.getParamValueBoolean(Arguments.local)
    val debugMode = clArgs.getParamValueBoolean(Arguments.debug)
    val firstSet = clArgs.getParamValueString(Arguments.firstSet)
    val firstSetObjType = clArgs.getParamValueString(Arguments.firstSetObjType)
    val secondSet = clArgs.getParamValueString(Arguments.secondSet)
    val secondSetObjType = clArgs.getParamValueString(Arguments.secondSetObjType)
    val outDir = clArgs.getParamValueString(Arguments.outDir)

    val kParam = clArgs.getParamValueInt(Arguments.k)

    val gridWidth = clArgs.getParamValueInt(Arguments.gridWidth)
    val partitionMaxByteSize = clArgs.getParamValueLong(Arguments.partitionMaxByteSize)

    val indexType = clArgs.getParamValueString(Arguments.indexType) match {
      case s if s.equalsIgnoreCase(SupportedSpatialIndexes.quadTree.toString) => SupportedSpatialIndexes.quadTree
      case s if s.equalsIgnoreCase(SupportedSpatialIndexes.kdTree.toString) => SupportedSpatialIndexes.kdTree
      case _ => throw new IllegalArgumentException("Unsupported spatial index type: %s".format(clArgs.getParamValueString(Arguments.indexType)))
    }

    val sparkConf = new SparkConf()

    if (localMode)
      sparkConf.setMaster("local[*]")
        .set("spark.local.dir", LocalRunConsts.sparkWorkDir)
        .set("spark.driver.memory", clArgs.getParamValueString(Arguments.driverMemory))
        .set("spark.executor.memory", clArgs.getParamValueString(Arguments.executorMemory))
        .set("spark.executor.instances", clArgs.getParamValueString(Arguments.numExecutors))
        .set("spark.executor.cores", clArgs.getParamValueString(Arguments.executorCores))

    val execAssignedMem = Helper.toByte(sparkConf.get("spark.executor.memory"))
    val driverAssignedMem = Helper.toByte(sparkConf.get("spark.driver.memory"))

    sparkConf.setAppName(this.getClass.getName)
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.executor.memoryOverhead", "%.0fb".format(execAssignedMem * 0.15))
      .set("spark.driver.memoryOverhead", "%.0fb".format(driverAssignedMem * 0.15))
      .set("spark.executor.heartbeatInterval", "40000")
      .set("spark.network.timeout", "150000")
      .registerKryoClasses(SparkKnn.getSparkKNNClasses)

    if (debugMode) {

      Helper.loggerSLf4J(debugMode, SparkKnn, ">>SparkConf: \n\t\t>>%s".format(sparkConf.getAll.mkString("\n\t\t>>")), null)
      Helper.loggerSLf4J(debugMode, SparkKnn, ">>CLArgs: \n\t\t>>%s".format(clArgs.toString), null)
    }

    val sc = new SparkContext(sparkConf)

    if (localMode)
      sc.setCheckpointDir("/var/tmp/spark_work_dir/checkpoints")

    val rddLeft = sc.textFile(firstSet)
      .mapPartitions(_.map(InputFileParsers.getLineParser(firstSetObjType)))
      .filter(_ != null)
      .mapPartitions(_.map(row => new Point(row._2._1.toDouble, row._2._2.toDouble, row._1)))

    val rddRight = sc.textFile(secondSet)
      .mapPartitions(_.map(InputFileParsers.getLineParser(secondSetObjType)))
      .filter(_ != null)
      .mapPartitions(_.map(row => new Point(row._2._1.toDouble, row._2._2.toDouble, row._1)))

    val sparkKNN = SparkKnn(debugMode, indexType, rddLeft, rddRight, kParam, gridWidth, partitionMaxByteSize)

    val rddResult = clArgs.getParamValueString(Arguments.knnJoinType) match {
      case s if s.equalsIgnoreCase(SupportedKnnOperations.knn.toString) => sparkKNN.knnJoin()
      case s if s.equalsIgnoreCase(SupportedKnnOperations.allKnn.toString) => sparkKNN.allKnnJoin()
      case _ => throw new IllegalArgumentException("Unsupported kNN join type: %s".format(clArgs.getParamValueString(Arguments.knnJoinType)))
    }

    // delete output dir if exists
    val hdfs = FileSystem.get(sc.hadoopConfiguration)
    val path = new Path(outDir)
    if (hdfs.exists(path)) hdfs.delete(path, true)

    //    Helper.loggerSLf4J(debugMode, SparkKnn, ">>toDebugString: \t%s".format(rddResult.toDebugString), null)

    rddResult.mapPartitions(_.map(row =>
      "%s;%s".format(row._1.userData, row._2.map(matchInfo =>
        "%.8f,%s".format(matchInfo._1, matchInfo._2.userData)).mkString(";"))))
      .saveAsTextFile(outDir, classOf[GzipCodec])

    if (clArgs.getParamValueBoolean(Arguments.local)) {

      val message = "%s\t%s\t%s\t%s\t%.4f%n".format("sKNN_" + indexType,
        firstSet.substring(firstSet.lastIndexOf("/") + 1),
        secondSet.substring(secondSet.lastIndexOf("/") + 1),
        outDir.substring(outDir.lastIndexOf("/") + 1),
        (System.currentTimeMillis() - startTime) / 1000.0
      )

      LocalRunConsts.logLocalRunEntry(LocalRunConsts.localRunLogFile, message, LocalRunConsts.localRunDebugLogFile, sparkKNN.lstDebugInfo)

      printf("Total Time: %,.4f Sec%n", (System.currentTimeMillis() - startTime) / 1000.0)
      println("Output: %s".format(outDir))
      println("Run Log: %s".format(LocalRunConsts.localRunLogFile))

      //            while (true) {
      //              print(". ")
      //              Thread.sleep(5000)
      //            }
    }
  }
}