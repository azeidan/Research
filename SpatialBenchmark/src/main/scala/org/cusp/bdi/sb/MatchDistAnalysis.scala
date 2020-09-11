package org.cusp.bdi.sb

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.cusp.bdi.sb.examples.{BenchmarkInputFileParser, SB_Arguments, SB_CLArgs}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object MatchDistAnalysis extends Serializable {

  //  private val LOGGER = LogFactory.getLog(this.getClass())

  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()

    //        val clArgs = SB_CLArgs.GM_LionTPEP
    //        val clArgs = SB_CLArgs.GM_LionTaxi
    //        val clArgs = SB_CLArgs.GS_LionTPEP
    //        val clArgs = SB_CLArgs.GS_LionTaxi
    //        val clArgs = SB_CLArgs.LS_LionTaxi
    //        val clArgs = SB_CLArgs.LS_LionBus
    //        val clArgs = SB_CLArgs.LS_LionTPEP
    //        val clArgs = SB_CLArgs.SKNN_BusPoint_BusPointShift
    val clArgs = SB_CLArgs.SKNN_RandomPoint_RandomPoint
    //        val clArgs = CLArgsParser(args, SB_Arguments())

    val classificationCount = clArgs.getParamValueInt(SB_Arguments.classificationCount)
    val testFWFileParser_1 = instantiateClass[BenchmarkInputFileParser](clArgs.getParamValueString(SB_Arguments.keyMatchInFileParser))
    val testFWFileParser_2 = instantiateClass[BenchmarkInputFileParser](clArgs.getParamValueString(SB_Arguments.testFWInFileParser))

    val sparkConf = new SparkConf().setAppName("Spatial Benchmark")

    if (clArgs.getParamValueBoolean(SB_Arguments.local))
      sparkConf.setMaster("local[*]")

    sparkConf.set("spark.serializer", classOf[KryoSerializer].getName)
    sparkConf.registerKryoClasses(Array(classOf[String]))

    val sparkContext = new SparkContext(sparkConf)

    // delete output dir if exists
    val hdfs = FileSystem.get(sparkContext.hadoopConfiguration)
    val path = new Path(clArgs.getParamValueString(SB_Arguments.outDir))
    if (hdfs.exists(path))
      hdfs.delete(path, true)

    val rdd1 = sparkContext.textFile(clArgs.getParamValueString(SB_Arguments.keyMatchInFile))
    val rdd2 = sparkContext.textFile(clArgs.getParamValueString(SB_Arguments.testFWInFile))

    val rddUnique1: RDD[(String, (Array[(Double, String)], Array[(Double, String)]))] =
      filterDuplicateRows(classificationCount, rdd1, testFWFileParser_1.parseLine)
        .mapPartitions(_.map(row => (row._1, (row._2, null))))

    val rddUnique2: RDD[(String, (Array[(Double, String)], Array[(Double, String)]))] =
      filterDuplicateRows(classificationCount, rdd2, testFWFileParser_2.parseLine)
        .mapPartitions(_.map(row => (row._1, (null, row._2))))

    val arrResults = rddUnique1.union(rddUnique2)
      .reduceByKey((x, y) => {

        var arrDS1 = x._1
        var arrDS2 = x._2

        if ((x._1 == null && x._2 == null) || y._1 == null && y._2 == null)
          throw new Exception("both arrays cannot be null") // shouldn't happen

        if (arrDS1 == null)
          arrDS1 = y._1

        if (arrDS2 == null)
          arrDS2 = y._2

        (arrDS1, arrDS2)
      })
      .mapPartitions(iter => {

        val mapClassifications = Classifications.getMapClassifications

        while (iter.hasNext) {

          val row = iter.next
          val (arr1, arr2) = (if (row._2._1 == null) Array[(Double, String)]() else row._2._1, if (row._2._2 == null) Array[(Double, String)]() else row._2._2)

          Classifications.incrementInMap("", arr1, arr2, mapClassifications, Classifications.recordsCount)

          //                    if (arr1 != null && arr2 == null)
          //                        Classifications.incrementInMap(mapClassifications, Classifications.recordsInFirstOnly)
          //                    else if (arr1 == null && arr2 != null)
          //                        Classifications.incrementInMap(mapClassifications, Classifications.recordsInSecondOnly)
          //                    else {

          if (arr1.length == 0 && arr2.length == 0)
            Classifications.incrementInMap("", arr1, arr2, mapClassifications, Classifications.recordsBothNoMatch)
          else if (arr1.length > 0 && arr2.length == 0)
            Classifications.incrementInMap(row._1, arr1, arr2, mapClassifications, Classifications.recordsInFirstOnly)
          else if (arr1.length == 0 && arr2.length > 0)
            Classifications.incrementInMap(row._1, arr1, arr2, mapClassifications, Classifications.recordsInSecondOnly)
          else if (arr1.length < arr2.length)
            Classifications.incrementInMap(row._1, arr1, arr2, mapClassifications, Classifications.recordsFirstUnderMatched)
          else if (arr1.length > arr2.length)
            Classifications.incrementInMap(row._1, arr1, arr2, mapClassifications, Classifications.recordsSecondUnderMatched)

          if (arr1.length > classificationCount && arr1.length > arr2.length)
            Classifications.incrementInMap(row._1, arr1, arr2, mapClassifications, Classifications.recordsFirstOverMatched)
          if (arr2.length > classificationCount && arr2.length > arr1.length)
            Classifications.incrementInMap(row._1, arr1, arr2, mapClassifications, Classifications.recordsSecondOverMatched)

          val agreeCount = (0 until math.min(arr1.length, arr2.length)).map(i => {

            // (countBoth, countInFirst, countInSecond)
            if (arr1(i)._1 == arr2(i)._1)
              (1, 0, 0)
            else if (arr1(i)._1 < arr2(i)._1)
              (0, 1, 0)
            else
              (0, 0, 1)
          })
            .reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))

          if (agreeCount._1 == classificationCount)
            Classifications.incrementInMap("", arr1, arr2, mapClassifications, Classifications.recordsSimilarMatch)
          if (agreeCount._2 > 0)
            Classifications.incrementInMap(row._1, arr1, arr2, mapClassifications, Classifications.recordsFirstBetter)
          if (agreeCount._3 > 0)
            Classifications.incrementInMap(row._1, arr1, arr2, mapClassifications, Classifications.recordsSecondBetter)

          //                    if (Classifications.missMatchFlag)
          //                        println(">>%s%n\t%s\n\t%s".format(row._1, arr1.mkString(","), arr2.mkString(",")))
        }
        //                }

        mapClassifications.iterator
      })
      .reduceByKey(_ + _)
      .collect()

    // add percentages
    val mapResults: mutable.Map[String, Long] = mutable.HashMap[String, Long]() ++ arrResults

    val recordsCount = mapResults(Classifications.recordsCount).toDouble
    val recordsBothNoMatch = mapResults(Classifications.recordsBothNoMatch).toDouble
    val recordsSimilarMatch = mapResults(Classifications.recordsSimilarMatch).toDouble
    val recordsInFirstOnly = mapResults(Classifications.recordsInFirstOnly).toDouble
    val recordsInSecondOnly = mapResults(Classifications.recordsInSecondOnly).toDouble
    val recordsFirstBetter = mapResults(Classifications.recordsFirstBetter).toDouble
    val recordsSecondBetter = mapResults(Classifications.recordsSecondBetter).toDouble
    val recordsFirstUnderMatched = mapResults(Classifications.recordsFirstUnderMatched).toDouble
    val recordsSecondUnderMatched = mapResults(Classifications.recordsSecondUnderMatched).toDouble
    val recordsFirstOverMatched = mapResults(Classifications.recordsFirstOverMatched).toDouble
    val recordsSecondOverMatched = mapResults(Classifications.recordsSecondOverMatched).toDouble

    // list to display in a specific order
    val lstResults = ListBuffer(getFormatted(mapResults, Classifications.recordsCount, recordsCount),
      getFormatted(mapResults, Classifications.recordsBothNoMatch, recordsBothNoMatch),
      getFormatted(mapResults, Classifications.percent, recordsBothNoMatch / recordsCount * 100),

      getFormatted(mapResults, Classifications.recordsSimilarMatch, recordsSimilarMatch),
      getFormatted(mapResults, Classifications.percent, recordsSimilarMatch / recordsCount * 100),

      getFormatted(mapResults, Classifications.recordsInFirstOnly, recordsInFirstOnly),
      getFormatted(mapResults, Classifications.percent, recordsInFirstOnly / recordsCount * 100),

      getFormatted(mapResults, Classifications.recordsInSecondOnly, recordsInSecondOnly),
      getFormatted(mapResults, Classifications.percent, recordsInSecondOnly / recordsCount * 100),

      getFormatted(mapResults, Classifications.recordsFirstBetter, recordsFirstBetter),
      getFormatted(mapResults, Classifications.percent, recordsFirstBetter / recordsCount * 100),

      getFormatted(mapResults, Classifications.recordsSecondBetter, recordsSecondBetter),
      getFormatted(mapResults, Classifications.percent, recordsSecondBetter / recordsCount * 100),

      getFormatted(mapResults, Classifications.recordsFirstUnderMatched, recordsFirstUnderMatched),
      getFormatted(mapResults, Classifications.percent, recordsFirstUnderMatched / recordsCount * 100),

      getFormatted(mapResults, Classifications.recordsSecondUnderMatched, recordsSecondUnderMatched),
      getFormatted(mapResults, Classifications.percent, recordsSecondUnderMatched / recordsCount * 100),

      getFormatted(mapResults, Classifications.recordsFirstOverMatched, recordsFirstOverMatched),
      getFormatted(mapResults, Classifications.percent, recordsFirstOverMatched / recordsCount * 100),

      getFormatted(mapResults, Classifications.recordsSecondOverMatched, recordsSecondOverMatched),
      getFormatted(mapResults, Classifications.percent, recordsSecondOverMatched / recordsCount * 100))

    sparkContext.parallelize(lstResults, 1)
      .saveAsTextFile(clArgs.getParamValueString(SB_Arguments.outDir), classOf[GzipCodec])

    if (clArgs.getParamValueBoolean(SB_Arguments.local)) {

      printf("Total Time: %,.2f Sec%n", (System.currentTimeMillis() - startTime) / 1000.0)
      println("Output idr: " + clArgs.getParamValueString(SB_Arguments.outDir))

      lstResults.foreach(println)
    }
  }

  private def getFormatted(mapResults: mutable.Map[String, Long], key: String, default: Any) = {

    val opt = mapResults.get(key)
    val value = if (opt.isEmpty) default else opt.get

    // Long data type assumed a count
    // Double data type assumed a percentage
    value match {
      case l: Long => "%41s".format(key) + ": " + "%,d".format(l)
      case d: Double => "%41s".format(key) + ": " + "%.4f%%".format(d)
      case _ => "%41s".format(key) + ": " + value
    }
  }

  private def filterDuplicateRows(classificationCount: Int, rdd: RDD[String], fileParser: String => (String, Array[String])) =
    rdd.mapPartitions(_.map(fileParser).filter(_ != null))
      .mapPartitions(_.map(row => {

        val arr = row._2.map(row => {
          val arr = row.split(',')
          (arr(0).toDouble, arr(1))
        })

        (row._1.toLowerCase(), arr)
      }))
      .reduceByKey((arr1, arr2) => extractOneArray(classificationCount, arr1, arr2))
      .mapPartitions(_.map(row => (row._1, row._2.asInstanceOf[Array[(Double, String)]])))

  private def extractOneArray(classificationCount: Int, arr1: Array[(Double, String)], arr2: Array[(Double, String)]) =
    (arr1.to[mutable.SortedSet] ++ arr2.to[mutable.SortedSet]).toArray.take(classificationCount)

  private def instantiateClass[T](className: String) = {

    var loadClass = className

    if (className.endsWith("$"))
      loadClass = className.substring(0, className.length() - 1)

    Class.forName(loadClass).getConstructor().newInstance().asInstanceOf[T]
  }

  object Classifications {

    //        var missMatchFlag = false

    val percent = "(%)"
    val recordsBothNoMatch = "Records both no match"
    val recordsCount = "Total Number of Records"
    val recordsSimilarMatch = "Records similarly matched"
    val recordsInFirstOnly = "Records matched in the 1st file only"
    val recordsInSecondOnly = "Records matched in the 2nd file only"
    val recordsFirstBetter = "Records in 1st file with closer matches"
    val recordsSecondBetter = "Records in 2nd file with closer matches"
    val recordsFirstUnderMatched = "Records undermatched in 1st file"
    val recordsSecondUnderMatched = "Records undermatched in 2nd file"
    val recordsFirstOverMatched = "Records over matched in 1st file"
    val recordsSecondOverMatched = "Records over matched in 2nd file"

    def getMapClassifications: mutable.HashMap[String, Long] = mutable.HashMap(Classifications.recordsBothNoMatch -> 0L,
      Classifications.recordsCount -> 0L,
      Classifications.recordsSimilarMatch -> 0L,
      Classifications.recordsInFirstOnly -> 0L,
      Classifications.recordsInSecondOnly -> 0L,
      Classifications.recordsFirstBetter -> 0L,
      Classifications.recordsSecondBetter -> 0L,
      Classifications.recordsFirstUnderMatched -> 0L,
      Classifications.recordsSecondUnderMatched -> 0L,
      Classifications.recordsFirstOverMatched -> 0L,
      Classifications.recordsSecondOverMatched -> 0L)

    def incrementInMap(row: String, arr1: Array[(Double, String)], arr2: Array[(Double, String)], mapClassifications: mutable.HashMap[String, Long], key: String) {

      //            if (key == recordsCount)
      //                missMatchFlag = false
      //            else if (key != recordsSimilarMatch)
      //                missMatchFlag = true

      mapClassifications.update(key, mapClassifications(key) + 1)
      if (row.length() > 0)
        println(">>%s%n\t%s\n\t%s".format(row, arr1.mkString(","), arr2.mkString(",")))
    }
  }

}