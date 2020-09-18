package org.cusp.bdi.sknn.test

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.cusp.bdi.ds.Point
import org.cusp.bdi.sknn.TypeSpatialIndex
import org.cusp.bdi.util.{Arguments, CLArgsParser, InputFileParsers}
import sknn.SparkKNN_Local_CLArgs
//import org.cusp.bdi.gm.GeoMatch
import org.cusp.bdi.sknn.SparkKNN
import org.cusp.bdi.sknn.util.RDD_Store
import org.cusp.bdi.util.LocalRunConsts

object TestAllKnnJoin {
  def main(args: Array[String]): Unit = {

    //    println(math.round(12.34))
    //    println(math.round(12.45))
    //    println(math.round(12.50))
    //    println(math.round(12.51))
    //    println(math.round(12.55))
    //System.exit(0)

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
    //    val outDirTmp = "%s%s%s".format(outDir, Path.SEPARATOR, "tmp")

    val kParam = clArgs.getParamValueInt(Arguments.k)
    val numPartitions = clArgs.getParamValueInt(Arguments.numPartitions)
    //        val sampleRate = clArgs.getParamValueDouble(Arguments.sampleRate)

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .set("spark.serializer", classOf[KryoSerializer].getName)
      //      .registerKryoClasses(GeoMatch.getGeoMatchClasses())
      .registerKryoClasses(SparkKNN.getSparkKNNClasses)

    if (localMode)
      sparkConf.setMaster("local[*]")
        .set("spark.local.dir", LocalRunConsts.sparkWorkDir)

    val sc = new SparkContext(sparkConf)

    val rddLeft = RDD_Store.getRDDPlain(sc, firstSet, numPartitions)
      .mapPartitions(_.map(InputFileParsers.getLineParser(firstSetObjType)))
      .filter(_ != null)
      .mapPartitions(_.map(row => new Point(row._2._1.toDouble, row._2._2.toDouble, row._1)))

    val rddRight = RDD_Store.getRDDPlain(sc, secondSet, numPartitions)
      .mapPartitions(_.map(InputFileParsers.getLineParser(secondSetObjType)))
      .filter(_ != null)
      .mapPartitions(_.map(row => new Point(row._2._1.toDouble, row._2._2.toDouble, row._1)))

    val sparkKNN = SparkKNN(debugMode, kParam, TypeSpatialIndex.quadTree)

    // during local test runs
    //    sparkKNN.numPartitions = numPartitions

    val rddResult = sparkKNN.allKnnJoin(rddLeft, rddRight)
    //        val rddResult = sparkKNN.knnJoin(rddLeft, rddRight)

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

      LocalRunConsts.logLocalRunEntry(LocalRunConsts.localRunLogFile, "sKNN",
        clArgs.getParamValueString(Arguments.firstSet).substring(clArgs.getParamValueString(Arguments.firstSet).lastIndexOf("/") + 1),
        clArgs.getParamValueString(Arguments.secondSet).substring(clArgs.getParamValueString(Arguments.secondSet).lastIndexOf("/") + 1),
        clArgs.getParamValueString(Arguments.outDir).substring(clArgs.getParamValueString(Arguments.outDir).lastIndexOf("/") + 1),
        (System.currentTimeMillis() - startTime) / 1000.0)

      printf("Total Time: %,.4f Sec%n", (System.currentTimeMillis() - startTime) / 1000.0)
      println("Output: %s".format(clArgs.getParamValueString(Arguments.outDir)))
      println("Run Log: %s".format(LocalRunConsts.localRunLogFile))
    }
  }
}