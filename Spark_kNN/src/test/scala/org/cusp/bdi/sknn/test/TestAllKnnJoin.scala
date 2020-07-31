package org.cusp.bdi.sknn.test

import com.insightfullogic.quad_trees.Point
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.cusp.bdi.gm.GeoMatch
import org.cusp.bdi.sknn.SparkKNN
import org.cusp.bdi.sknn.util.{RDD_Store, SparkKNN_Arguments}
import org.cusp.bdi.util.{Helper, LocalRunConsts}
import org.cusp.bdi.util.sknn.SparkKNN_Local_CLArgs

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

    val clArgs = SparkKNN_Local_CLArgs.random_sample(SparkKNN_Arguments())
    //    val clArgs = SparkKNN_Local_CLArgs.busPoint_busPointShift(SparkKNN_Arguments())
    //    val clArgs = SparkKNN_Local_CLArgs.busPoint_taxiPoint(SparkKNN_Arguments())
    //    val clArgs = SparkKNN_Local_CLArgs.tpepPoint_tpepPoint(SparkKNN_Arguments())
    //    val clArgs = CLArgsParser(args, SparkKNN_Arguments())

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
    val outDir = clArgs.getParamValueString(SparkKNN_Arguments.outDir)

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .registerKryoClasses(GeoMatch.getGeoMatchClasses())
      .registerKryoClasses(SparkKNN.getSparkKNNClasses())

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

    val sparkKNN = SparkKNN(rddLeft, rddRight /*, sampleRate*/ , kParam)

    // during local test runs
    sparkKNN.minPartitions = minPartitions

    //        val rddResult = sparkKNN.allKnnJoin()
    val rddResult = sparkKNN.allKnnJoin()

    // delete output dir if exists
    Helper.delDirHDFS(rddResult.context, clArgs.getParamValueString(SparkKNN_Arguments.outDir))

    rddResult.mapPartitions(_.map(row =>
      "%s,%.8f,%.8f;%s".format(row._1.userData, row._1.x, row._1.y, row._2.map(matchInfo =>
        "%.8f,%s".format(math.sqrt(matchInfo._1), matchInfo._2.userData)).mkString(";"))))
      .saveAsTextFile(outDir, classOf[GzipCodec])

    if (clArgs.getParamValueBoolean(SparkKNN_Arguments.local)) {

      printf("Total Time: %,.2f Sec%n", (System.currentTimeMillis() - startTime) / 1000.0)

      println(outDir)
    }
  }
}