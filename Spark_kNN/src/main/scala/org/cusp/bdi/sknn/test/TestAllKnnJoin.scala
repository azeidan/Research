package org.cusp.bdi.sknn.test

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.cusp.bdi.ds.Point
import org.cusp.bdi.util.sknn.SparkKNN_Local_CLArgs
//import org.cusp.bdi.gm.GeoMatch
import org.cusp.bdi.sknn.SparkKNN
import org.cusp.bdi.sknn.util.{RDD_Store, SparkKNN_Arguments}
import org.cusp.bdi.util.{CLArgsParser, LocalRunConsts}

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

//        val clArgs = SparkKNN_Local_CLArgs.random_sample(SparkKNN_Arguments())
    val clArgs = CLArgsParser(args, SparkKNN_Arguments())

    //    val clArgs = SparkKNN_Local_CLArgs.busPoint_busPointShift(SparkKNN_Arguments())
    //    val clArgs = SparkKNN_Local_CLArgs.busPoint_taxiPoint(SparkKNN_Arguments())
    //    val clArgs = SparkKNN_Local_CLArgs.tpepPoint_tpepPoint(SparkKNN_Arguments())

    val localMode = clArgs.getParamValueBoolean(SparkKNN_Arguments.local)
    val firstSet = clArgs.getParamValueString(SparkKNN_Arguments.firstSet)
    val firstSetObjType = clArgs.getParamValueString(SparkKNN_Arguments.firstSetObjType)
    val firstSetParser = RDD_Store.getLineParser(firstSetObjType)
    val secondSet = clArgs.getParamValueString(SparkKNN_Arguments.secondSet)
    val secondSetObjType = clArgs.getParamValueString(SparkKNN_Arguments.secondSetObjType)
    val secondSetParser = RDD_Store.getLineParser(secondSetObjType)

    val kParam = clArgs.getParamValueInt(SparkKNN_Arguments.k)
    val minPartitions = clArgs.getParamValueInt(SparkKNN_Arguments.minPartitions)
    //        val sampleRate = clArgs.getParamValueDouble(SparkKNN_Arguments.sampleRate)

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .set("spark.serializer", classOf[KryoSerializer].getName)
      //      .registerKryoClasses(GeoMatch.getGeoMatchClasses())
      .registerKryoClasses(SparkKNN.getSparkKNNClasses)

    if (localMode)
      sparkConf.setMaster("local[*]")
        .set("spark.local.dir", LocalRunConsts.sparkWorkDir)

    val sc = new SparkContext(sparkConf)

    val rddLeft = RDD_Store.getRDDPlain(sc, firstSet, minPartitions)
      .mapPartitions(_.map(firstSetParser))
      .filter(_ != null)
      .mapPartitions(_.map(row => {

        val point = new Point(row._2._1.toDouble, row._2._2.toDouble)
        point.userData = row._1

        point
      }))

    val rddRight = RDD_Store.getRDDPlain(sc, secondSet, minPartitions)
      .mapPartitions(_.map(secondSetParser))
      .filter(_ != null)
      .mapPartitions(_.map(row => {

        val point = new Point(row._2._1.toDouble, row._2._2.toDouble)
        point.userData = row._1

        point
      }))

    val sparkKNN = SparkKNN(rddLeft, rddRight  , kParam)

    // during local test runs
    sparkKNN.minPartitions = minPartitions

    //        val rddResult = sparkKNN.allKnnJoin()
    val rddResult = sparkKNN.allKnnJoin()

    // delete output dir if exists
    val hdfs = FileSystem.get(sc.hadoopConfiguration)
    val path = new Path(clArgs.getParamValueString(SparkKNN_Arguments.outDir))
    if (hdfs.exists(path))
      hdfs.delete(path, true)

    rddResult.mapPartitions(_.map(row =>
      "%s,%.8f,%.8f;%s".format(row._1.userData, row._1.x, row._1.y, row._2.map(matchInfo =>
        "%.8f,%s".format(math.sqrt(matchInfo._1), matchInfo._2.userData)).mkString(";"))))
      .saveAsTextFile(clArgs.getParamValueString(SparkKNN_Arguments.outDir), classOf[GzipCodec])

    if (clArgs.getParamValueBoolean(SparkKNN_Arguments.local)) {

      LocalRunConsts.logLocalRunEntry(LocalRunConsts.localRunLogFile, "sKNN",
        clArgs.getParamValueString(SparkKNN_Arguments.firstSet).substring(clArgs.getParamValueString(SparkKNN_Arguments.firstSet).lastIndexOf("/") + 1),
        clArgs.getParamValueString(SparkKNN_Arguments.secondSet).substring(clArgs.getParamValueString(SparkKNN_Arguments.secondSet).lastIndexOf("/") + 1),
        clArgs.getParamValueString(SparkKNN_Arguments.outDir).substring(clArgs.getParamValueString(SparkKNN_Arguments.outDir).lastIndexOf("/") + 1),
        (System.currentTimeMillis() - startTime) / 1000.0)

      printf("Total Time: %,.4f Sec%n", (System.currentTimeMillis() - startTime) / 1000.0)
      println("Output: %s".format(clArgs.getParamValueString(SparkKNN_Arguments.outDir)))
      println("Run Log: %s".format(LocalRunConsts.localRunLogFile))
    }
  }
}