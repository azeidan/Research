
package org.cusp.bdi.sb.test

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.cusp.bdi.sb.test.OutputsCompare.COL_WIDTH
import org.cusp.bdi.util.Helper

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object OutputsCompare {

  val percent = "(%)"
  val recordsBothNoMatch = "Records both no match"
  val recordsCount = "Total Number of Records"
  val recordsFWCorrectMatched = "Records correctly matched"
  val recordsFWFailedToMatch = "Records framework failed to match"
  val recordsFWMismatch = "Records framework incorrectly matched"
  val recordsFWOnlyMatched = "Records framework only matched"
  val recordsFWOverMatched = "Records framework overmatched"
  val recordsFWUnderMatched = "Records framework undermatched"
  val recordsInFWOnly = "Records appeared in framework only"
  val recordsInKMOnly = "Records appeared in key match only"

  val COL_WIDTH = Array(recordsBothNoMatch.length, recordsCount.length, recordsFWCorrectMatched.length, recordsFWFailedToMatch.length, recordsFWMismatch.length,
    recordsFWOnlyMatched.length, recordsFWOverMatched.length, recordsFWUnderMatched.length, recordsInFWOnly.length, recordsInKMOnly.length)
    .max
}

case class OutputsCompare(classificationCount: Int, rddKeyMatch: RDD[String],
                          keyMatchFileParserClass: String,
                          rddTestFW: RDD[String],
                          testFWFileParserClass: String) extends Serializable {

  val keyMatchFileParser: BenchmarkInputFileParser = instantiateParser(keyMatchFileParserClass)
  val testFWFileParser: BenchmarkInputFileParser = instantiateParser(testFWFileParserClass)

  def compare(): ListBuffer[String] = {

    val rddKeyMatchUnique: RDD[(String, (Array[String], Array[String]))] = rddUnique(rddKeyMatch, keyMatchFileParser.parseLine)
      .mapPartitions(_.map(x => {

        val arr: Array[String] = null

        (x._1, (x._2, arr))
      }))

    val rddTestFWUnique: RDD[(String, (Array[String], Array[String]))] = rddUnique(rddTestFW, testFWFileParser.parseLine)
      .mapPartitions(_.map(x => {

        val arr: Array[String] = null

        (x._1, (arr, x._2))
      }))

    val arrResults: Array[(String, Long)] = rddKeyMatchUnique.union(rddTestFWUnique)
      .reduceByKey((x, y) => {

        var arr1 = x._1
        var arr2 = x._2

        if (x._1 == null)
          arr1 = y._1
        else if (y._1 != null)
          arr1 = x._1 ++ y._1

        if (x._2 == null)
          arr2 = y._2
        else if (y._2 != null)
          arr1 = x._2 ++ y._2

        (arr1, arr2)
      })
      .mapPartitions(iter => {

        val mapRowLevelClassify = mutable.HashMap(OutputsCompare.recordsBothNoMatch -> 0L,
          OutputsCompare.recordsCount -> 0L,
          OutputsCompare.recordsFWCorrectMatched -> 0L,
          OutputsCompare.recordsFWFailedToMatch -> 0L,
          OutputsCompare.recordsFWMismatch -> 0L,
          OutputsCompare.recordsFWOnlyMatched -> 0L,
          OutputsCompare.recordsFWOverMatched -> 0L,
          OutputsCompare.recordsFWUnderMatched -> 0L,
          OutputsCompare.recordsInFWOnly -> 0L,
          OutputsCompare.recordsInKMOnly -> 0L)

        def incrementInMap(classificationKey: String, row: (String, (Array[String], Array[String]))): Unit = {

          if (!(classificationKey.equals(OutputsCompare.recordsCount) || classificationKey.equals(OutputsCompare.recordsBothNoMatch) || classificationKey.equals(OutputsCompare.recordsFWCorrectMatched)))
            println(">>\t%s: %s\n\t\t\t>>%s\n\t\t\t>>%s".format(classificationKey, row._1, if (row._2._1 == null) "" else row._2._1.mkString(","), if (row._2._2 == null) "" else row._2._2.mkString(",")))

          mapRowLevelClassify.update(classificationKey, mapRowLevelClassify(classificationKey) + 1)
        }

        iter.foreach(row => {

          if (row._2._1 != null)
            incrementInMap(OutputsCompare.recordsCount, row)

          var done = false

          if (row._2._1 == null && row._2._2 != null) {

            incrementInMap(OutputsCompare.recordsInFWOnly, row)
            done = true
          }
          else if (row._2._1 != null && row._2._2 == null) {

            incrementInMap(OutputsCompare.recordsInKMOnly, row)
            done = true
          }

          if (!done) {

            val arrKMSize = row._2._1.length
            val arrFWSize = row._2._2.length

            val arrMatchIdxs = row._2._2.map(_ => -1)

            row._2._2.indices.foreach(i => arrMatchIdxs(i) = row._2._1.indexOf(row._2._2(i)))

            lazy val kmDistances = row._2._1.map(_.split(",")).map(_ (0))

            row._2._2.indices
              .filter(arrIdx => arrMatchIdxs(arrIdx) == -1)
              .foreach(arrIdx => {

                val dist = row._2._2(arrIdx).split(",")(0)

                val arrValidIdxs = kmDistances
                  .indices
                  .filter(i => kmDistances(i).equals(dist))
                  .filterNot(idx => arrMatchIdxs.contains(idx)).take(1)

                if (arrValidIdxs.nonEmpty)
                  arrMatchIdxs(arrIdx) = arrValidIdxs.head
              })

            if (arrFWSize == 0 && arrKMSize == 0) {
              incrementInMap(OutputsCompare.recordsBothNoMatch, row)
              incrementInMap(OutputsCompare.recordsFWCorrectMatched, row)
            }
            else if (arrFWSize > 0 && arrKMSize == 0)
              incrementInMap(OutputsCompare.recordsFWOnlyMatched, row)
            else if (arrFWSize == 0 && arrKMSize > 0)
              incrementInMap(OutputsCompare.recordsFWFailedToMatch, row)
            else if (arrFWSize > arrKMSize)
              incrementInMap(OutputsCompare.recordsFWOverMatched, row)
            else if (arrFWSize < classificationCount && arrFWSize < arrKMSize)
              incrementInMap(OutputsCompare.recordsFWUnderMatched, row)

            var correctStreetCount = 0

            correctStreetCount = arrMatchIdxs.take(classificationCount)
              .seq
              .groupBy(identity)
              .mapValues(_.size)
              .toArray
              .sortBy(_._1)
              .map(tuple => if ((0 until classificationCount).contains(tuple._1)) 1 else 0).sum

            // add # "Records both no match" to # of "Records correctly matched" since they are correctly classified
            if (correctStreetCount == classificationCount)
              incrementInMap(OutputsCompare.recordsFWCorrectMatched, row)
            else
              incrementInMap(OutputsCompare.recordsFWMismatch, row)
          }
        })

        mapRowLevelClassify.iterator
      })
      .reduceByKey(_ + _)
      .collect()

    // add percentages
    val mapResults: mutable.HashMap[String, Long] = mutable.HashMap[String, Long]()

    arrResults.foreach(x => mapResults += x._1 -> x._2)

    val recordsCount: Double = mapResults(OutputsCompare.recordsCount).toDouble
    val recordsBothNoMatch: Double = mapResults(OutputsCompare.recordsBothNoMatch).toDouble
    val recordsFWOnlyMatched: Double = mapResults(OutputsCompare.recordsFWOnlyMatched).toDouble
    val recordsFWCorrectMatched: Double = mapResults(OutputsCompare.recordsFWCorrectMatched).toDouble
    val recordsFWFailedToMatch: Double = mapResults(OutputsCompare.recordsFWFailedToMatch).toDouble
    val recordsFWOverMatched: Double = mapResults(OutputsCompare.recordsFWOverMatched).toDouble
    val recordsFWUnderMatched: Double = mapResults(OutputsCompare.recordsFWUnderMatched).toDouble
    val recordsInFWOnly: Double = mapResults(OutputsCompare.recordsInFWOnly).toDouble
    val recordsInKMOnly: Double = mapResults(OutputsCompare.recordsInKMOnly).toDouble
    val recordsFWMismatch: Double = mapResults(OutputsCompare.recordsFWMismatch).toDouble

    // list to display in a specific order
    ListBuffer(getFormatted(mapResults, OutputsCompare.recordsCount, recordsCount),
      getFormatted(mapResults, OutputsCompare.recordsFWCorrectMatched, recordsFWCorrectMatched),
      getFormatted(mapResults, OutputsCompare.percent, recordsFWCorrectMatched / recordsCount * 100),
      getFormatted(mapResults, OutputsCompare.recordsBothNoMatch, recordsBothNoMatch),
      getFormatted(mapResults, OutputsCompare.percent, recordsBothNoMatch / recordsCount * 100),
      getFormatted(mapResults, OutputsCompare.recordsFWOnlyMatched, recordsFWOnlyMatched),
      getFormatted(mapResults, OutputsCompare.percent, recordsFWOnlyMatched / recordsCount * 100),
      getFormatted(mapResults, OutputsCompare.recordsFWFailedToMatch, recordsFWFailedToMatch),
      getFormatted(mapResults, OutputsCompare.percent, recordsFWFailedToMatch / recordsCount * 100),
      getFormatted(mapResults, OutputsCompare.recordsFWOverMatched, recordsFWOverMatched),
      getFormatted(mapResults, OutputsCompare.percent, recordsFWOverMatched / recordsCount * 100),
      getFormatted(mapResults, OutputsCompare.recordsFWUnderMatched, recordsFWUnderMatched),
      getFormatted(mapResults, OutputsCompare.percent, recordsFWUnderMatched / recordsCount * 100),
      getFormatted(mapResults, OutputsCompare.recordsInFWOnly, recordsInFWOnly),
      getFormatted(mapResults, OutputsCompare.percent, recordsInFWOnly / recordsCount * 100),
      getFormatted(mapResults, OutputsCompare.recordsInKMOnly, recordsInKMOnly),
      getFormatted(mapResults, OutputsCompare.percent, recordsInKMOnly / recordsCount * 100),
      getFormatted(mapResults, OutputsCompare.recordsFWMismatch, recordsFWMismatch),
      getFormatted(mapResults, OutputsCompare.percent, recordsFWMismatch / recordsCount * 100))
  }

  private def rddUnique(rdd: RDD[String], fileParser: String => (String, Array[String])) =
    rdd.mapPartitions(_.map(fileParser).filter(_ != null))
      .mapPartitions(_.map(x => {

        val arr = if (x._2 == null) Array[String]() else x._2.filter(x => !Helper.isNullOrEmpty(x))

        (x._1.toLowerCase(), arr)
      }))
      .reduceByKey((x, y) => extractOneArray(x, y))

  private def extractOneArray(arr0: Array[String], arr1: Array[String]) = {

    val arrFilter0 = arr0.filter(x => !Helper.isNullOrEmpty(x))
    val arrFilter1 = arr1.filter(x => !Helper.isNullOrEmpty(x))

    if (Helper.isNullOrEmpty(arrFilter0))
      arrFilter1
    else if (Helper.isNullOrEmpty(arrFilter1))
      arrFilter0
    else {

      val lst = arrFilter0.to[ListBuffer]

      arrFilter1.foreach(x => lst.append(x))

      lst.distinct.toArray
    }
  }

  private def getFormatted(mapResults: mutable.HashMap[String, Long], key: String, default: AnyVal) =
    mapResults.getOrElse(key, default) match {
      // Long data type assumed a count
      // Double data type assumed a percentage
      case l: Long => ("%" + COL_WIDTH + "s: %,d").format(key, l)
      case d: Double => ("%" + COL_WIDTH + "s: %.8e%%").format(key, d)
      case other => ("%" + COL_WIDTH + "s: %s").format(key, other)
    }

  private def instantiateParser(className: String): BenchmarkInputFileParser = {

    var loadClass = className

    if (className.endsWith("$"))
      loadClass = className.substring(0, className.length() - 1)

    Class.forName(loadClass).getConstructor().newInstance().asInstanceOf[BenchmarkInputFileParser]
  }
}