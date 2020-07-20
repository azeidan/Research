package org.cusp.bdi.tools.spark

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkConf, SparkContext}
import org.cusp.bdi.util.LocalRunConsts

object RandomSampler {

  var localMode = true
  val outDir = "/media/ayman/Data/GeoMatch_Files/OutputFiles/000/"
  var numRows = 1e3.toInt

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getName)

    if (localMode)
      sparkConf.setMaster("local[*]")
        .set("spark.local.dir", LocalRunConsts.sparkWorkDir)

    val sc = new SparkContext(sparkConf)

    def getSample(inputFile: String, outputFile: String, numRows: Int) {

      val rddLeft = sc.textFile(inputFile)

      sc.parallelize(rddLeft.takeSample(false, numRows))
        .saveAsTextFile(outputFile, classOf[GzipCodec])
    }

    Array(("/gws/projects/project-taxi_capstone_2016/share/Bus_TripRecod_NAD83/", "Bus"),
      ("/gws/projects/project-taxi_capstone_2016/data/breadcrumb_nad83/", "Bread"))
      .map(row => Iterator((row._1, row._2 + "_1"), (row._1, row._2 + "_2"), (row._1, row._2 + "_3")))
      .flatMap(_.seq)
      .foreach(row =>
        getSample(row._1, outDir + row._2.substring(row._2.lastIndexOf("/")), numRows))

  }
}
