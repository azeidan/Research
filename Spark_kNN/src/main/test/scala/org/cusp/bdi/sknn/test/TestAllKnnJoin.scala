package org.cusp.bdi.sknn.test

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.cusp.bdi.ds.geom.Point
import org.cusp.bdi.sknn.ds.util.SupportedSpatialIndexes
import org.cusp.bdi.sknn.{SparkKnn, SupportedKnnOperations}
import org.cusp.bdi.util.{Arguments, CLArgsParser, Helper, InputFileParsers, LocalRunConsts}

object TestAllKnnJoin {

  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()
    //    var startTime2 = startTime

//    val clArgs = SparkKNN_Local_CLArgs.random_sample()
                val clArgs = CLArgsParser(args, Arguments.lstArgInfo())

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

    val indexType = clArgs.getParamValueString(Arguments.indexType) match {
      case s if s.equalsIgnoreCase(SupportedSpatialIndexes.quadTree.toString) => SupportedSpatialIndexes.quadTree
      case s if s.equalsIgnoreCase(SupportedSpatialIndexes.kdTree.toString) => SupportedSpatialIndexes.kdTree
      case _ => throw new IllegalArgumentException("Unsupported spatial index type: %s".format(clArgs.getParamValueString(Arguments.indexType)))
    }

    //    val numPartitions = if (clArgs.getParamValueBoolean(Arguments.local)) 17 else 0

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .registerKryoClasses(SparkKnn.getSparkKNNClasses)

    if (localMode)
      sparkConf.setMaster("local[*]")
        .set("spark.local.dir", LocalRunConsts.sparkWorkDir)
        .set("spark.driver.memory", clArgs.getParamValueString(Arguments.driverMemory))
        .set("spark.executor.memory", clArgs.getParamValueString(Arguments.executorMemory))
        .set("spark.executor.instances", clArgs.getParamValueString(Arguments.numExecutors))
        .set("spark.executor.cores", clArgs.getParamValueString(Arguments.executorCores))

    if (debugMode)
      Helper.loggerSLf4J(debugMode, SparkKnn, ">>SparkConf: \n\t\t>>%s".format(sparkConf.getAll.mkString("\n\t\t>>")), null)

    val sc = new SparkContext(sparkConf)

    val rddLeft = sc.textFile(firstSet)
      .mapPartitions(_.map(InputFileParsers.getLineParser(firstSetObjType)))
      .filter(_ != null)
      .mapPartitions(_.map(row => new Point(row._2._1.toDouble, row._2._2.toDouble, row._1)))

    val rddRight = sc.textFile(secondSet)
      .mapPartitions(_.map(InputFileParsers.getLineParser(secondSetObjType)))
      .filter(_ != null)
      .mapPartitions(_.map(row => new Point(row._2._1.toDouble, row._2._2.toDouble, row._1)))

    val sparkKNN = SparkKnn(debugMode, indexType, rddLeft, rddRight, kParam)

    val rddResult = clArgs.getParamValueString(Arguments.knnJoinType) match {
      case s if s.equalsIgnoreCase(SupportedKnnOperations.knn.toString) => sparkKNN.knnJoin()
      case s if s.equalsIgnoreCase(SupportedKnnOperations.allKnn.toString) => sparkKNN.allKnnJoin()
      case _ => throw new IllegalArgumentException("Unsupported kNN join type: %s".format(clArgs.getParamValueString(Arguments.knnJoinType)))
    }

    //    println(rddResult.toDebugString)

    // delete output dir if exists
    val hdfs = FileSystem.get(sc.hadoopConfiguration)
    val path = new Path(outDir)
    if (hdfs.exists(path)) hdfs.delete(path, true)

    rddResult.mapPartitions(_.map(row =>
      "%s,%.8f,%.8f;%s".format(row._1.userData, row._1.x, row._1.y, row._2.map(matchInfo =>
        "%.8f,%s".format(math.sqrt(matchInfo._1), matchInfo._2.userData)).mkString(";"))))
      .saveAsTextFile(outDir, classOf[GzipCodec])

    if (clArgs.getParamValueBoolean(Arguments.local)) {

      LocalRunConsts.logLocalRunEntry(LocalRunConsts.localRunLogFile, "sKNN_" + indexType,
        clArgs.getParamValueString(Arguments.firstSet).substring(clArgs.getParamValueString(Arguments.firstSet).lastIndexOf("/") + 1),
        clArgs.getParamValueString(Arguments.secondSet).substring(clArgs.getParamValueString(Arguments.secondSet).lastIndexOf("/") + 1),
        clArgs.getParamValueString(Arguments.outDir).substring(clArgs.getParamValueString(Arguments.outDir).lastIndexOf("/") + 1),
        (System.currentTimeMillis() - startTime) / 1000.0,
        LocalRunConsts.localRunDebugLogFile,
        sparkKNN.lstDebugInfo)

      printf("Total Time: %,.4f Sec%n", (System.currentTimeMillis() - startTime) / 1000.0)
      println("Output: %s".format(clArgs.getParamValueString(Arguments.outDir)))
      println("Run Log: %s".format(LocalRunConsts.localRunLogFile))
    }
  }
}