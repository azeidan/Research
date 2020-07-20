package org.cusp.bdi.tools.spark

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkConf, SparkContext}
import org.cusp.bdi.util.Helper

object LineCount {
  def main(args: Array[String]): Unit = {

    if (args.length != 3)
      println("Usage: <input file> <utput dir> <run locally>")
    else
      allLineCount(args(0), args(1), Helper.isBooleanStr(args(2)))
  }

  def allLineCount(fileName: String, output: String, runLocal: Boolean) {

    val startTime = System.currentTimeMillis()

    val appName = this.getClass.getName + "_" + fileName
    var outputDir = output

    val conf = new SparkConf().setAppName(appName)

    if (runLocal) {

      conf.setMaster("local[*]")

      // randomize output dir
      outputDir = Helper.randOutputDir(outputDir)
    }

    val sc = new SparkContext(conf)

    val lineCount = sc.textFile(fileName)
      .mapPartitions(_.map(line => {

        if (line.length() > 0)
          1L
        else
          0L
      }), true)
      .filter(_ != 0)
      .sum()

    val distinctLineCount = sc.textFile(fileName)
      .mapPartitions(_.map(line => {

        if (line.length() > 0)
          (line, 1L)
        else
          (line, 0L)
      }), true)
      .filter(_._2 != 0)
      .reduceByKey((x, y) => x)
      .mapPartitions(_.map(_._2), true)
      .sum()

    sc.parallelize(Seq("All Lines: " + lineCount + "\nDistinct Lines: " + distinctLineCount))
      .saveAsTextFile(outputDir, classOf[GzipCodec])

    if (runLocal) {

      println("Total Time: " + (System.currentTimeMillis() - startTime))
      println("Output idr: " + outputDir)
    }
  }
}
