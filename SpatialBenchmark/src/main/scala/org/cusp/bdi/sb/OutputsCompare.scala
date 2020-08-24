
package org.cusp.bdi.sb

import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.cusp.bdi.util.Helper
import org.cusp.bdi.sb.examples.BenchmarkInputFileParser
import org.apache.commons.lang3.builder.HashCodeBuilder

import scala.collection.JavaConverters._
import scala.collection.mutable

object OutputsCompare extends Serializable {

  def apply(classificationCount: Int, rddKeyMatch: RDD[String], keyMatchFileParser: BenchmarkInputFileParser, rddTestFW: RDD[String], testFWFileParser: BenchmarkInputFileParser): ListBuffer[String] = {

    val rddKeyMatchUnique = rddUnique(rddKeyMatch, keyMatchFileParser.parseLine)
      .mapPartitions(_.map(x => {

        val arr: Array[String] = null

        (x._1, (x._2, arr))
      }))

    val rddTestFWUnique = rddUnique(rddTestFW, testFWFileParser.parseLine)
      .mapPartitions(_.map(x => {

        val arr: Array[String] = null

        (x._1, (arr, x._2))
      }))

    val arrResults = rddKeyMatchUnique.union(rddTestFWUnique)
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

        val mapRowLevelClassify = mutable.HashMap(Classifications.recordsBothNoMatch -> 0L,
          Classifications.recordsCount -> 0L,
          Classifications.recordsFWCorrectMatched -> 0L,
          Classifications.recordsFWFailedToMatch -> 0L,
          Classifications.recordsFWMismatch -> 0L,
          Classifications.recordsFWOnlyMatched -> 0L,
          Classifications.recordsFWOverMatched -> 0L,
          Classifications.recordsFWUnderMatched -> 0L,
          Classifications.recordsInFWOnly -> 0L,
          Classifications.recordsInKMOnly -> 0L)

        def incrementInMap(classificationKey: String, row: (String, (Array[String], Array[String]))): Unit = {

          if (!(classificationKey.equals(Classifications.recordsCount) || classificationKey.equals(Classifications.recordsBothNoMatch) || classificationKey.equals(Classifications.recordsFWCorrectMatched)))
            println(">>\t%s: %s\n\t\t\t>>%s\n\t\t\t>>%s".format(classificationKey, row._1, if (row._2._1 == null) "" else row._2._1.mkString(","), if (row._2._2 == null) "" else row._2._2.mkString(",")))

          mapRowLevelClassify.update(classificationKey, mapRowLevelClassify(classificationKey) + 1)
        }

        iter.foreach(row => {

//          if (row._1.startsWith("bread_1_b_817301".toLowerCase))
//            println()

          if (row._2._1 != null)
            incrementInMap(Classifications.recordsCount, row)

          var done = false

          if (row._2._1 == null && row._2._2 != null) {

            incrementInMap(Classifications.recordsInFWOnly, row)
            done = true
          }
          else if (row._2._1 != null && row._2._2 == null) {

            incrementInMap(Classifications.recordsInKMOnly, row)
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

                val l = kmDistances
                  .indices
                  .filter(i => kmDistances(i).equals(dist))

                val arrValidIdxs = l.filterNot(idx => arrMatchIdxs.contains(idx)).take(1)

                //                val arrValidIdxs = ll.find(i => arrMatchIdxs(i) == -1)
                //                  .getOrElse(-1)

                if (arrValidIdxs.nonEmpty)
                  arrMatchIdxs(arrIdx) = arrValidIdxs.head
              })

            if (arrFWSize == 0 && arrKMSize == 0) {
              incrementInMap(Classifications.recordsBothNoMatch, row)
              incrementInMap(Classifications.recordsFWCorrectMatched, row)
            }
            else if (arrFWSize > 0 && arrKMSize == 0)
              incrementInMap(Classifications.recordsFWOnlyMatched, row)
            else if (arrFWSize == 0 && arrKMSize > 0)
              incrementInMap(Classifications.recordsFWFailedToMatch, row)
            else if (arrFWSize > arrKMSize)
              incrementInMap(Classifications.recordsFWOverMatched, row)
            else if (arrFWSize < classificationCount && arrFWSize < arrKMSize)
              incrementInMap(Classifications.recordsFWUnderMatched, row)

            var correctStreetCount = 0

            correctStreetCount = arrMatchIdxs.take(classificationCount)
              .seq
              .groupBy(identity)
              .mapValues(_.size)
              .toArray
              .sortBy(_._1)
              .map(tuple => {

                if ((0 until classificationCount).contains(tuple._1))
                  1
                else
                  0
              }).sum

            // add # "Records both no match" to # of "Records correctly matched" since they are correctly classified
            if (correctStreetCount == classificationCount)
              incrementInMap(Classifications.recordsFWCorrectMatched, row)
            else
              incrementInMap(Classifications.recordsFWMismatch, row)
          }
        })

        mapRowLevelClassify.iterator
      })
      .reduceByKey(_ + _)
      .collect()

    // add percentages
    val mapResults = mutable.HashMap[String, Long]()

    arrResults.foreach(x => mapResults += x._1 -> x._2)

    val recordsCount = mapResults(Classifications.recordsCount).toDouble
    val recordsBothNoMatch = mapResults(Classifications.recordsBothNoMatch).toDouble
    val recordsFWOnlyMatched = mapResults(Classifications.recordsFWOnlyMatched).toDouble
    val recordsFWCorrectMatched = mapResults(Classifications.recordsFWCorrectMatched).toDouble
    val recordsFWFailedToMatch = mapResults(Classifications.recordsFWFailedToMatch).toDouble
    val recordsFWOverMatched = mapResults(Classifications.recordsFWOverMatched).toDouble
    val recordsFWUnderMatched = mapResults(Classifications.recordsFWUnderMatched).toDouble
    val recordsInFWOnly = mapResults(Classifications.recordsInFWOnly).toDouble
    val recordsInKMOnly = mapResults(Classifications.recordsInKMOnly).toDouble
    val recordsFWMismatch = mapResults(Classifications.recordsFWMismatch).toDouble

    // list to display in a specific order
    ListBuffer(getFormatted(mapResults, Classifications.recordsCount, recordsCount),
      getFormatted(mapResults, Classifications.recordsFWCorrectMatched, recordsFWCorrectMatched),
      getFormatted(mapResults, Classifications.percent, recordsFWCorrectMatched / recordsCount * 100),
      getFormatted(mapResults, Classifications.recordsBothNoMatch, recordsBothNoMatch),
      getFormatted(mapResults, Classifications.percent, recordsBothNoMatch / recordsCount * 100),
      getFormatted(mapResults, Classifications.recordsFWOnlyMatched, recordsFWOnlyMatched),
      getFormatted(mapResults, Classifications.percent, recordsFWOnlyMatched / recordsCount * 100),
      getFormatted(mapResults, Classifications.recordsFWFailedToMatch, recordsFWFailedToMatch),
      getFormatted(mapResults, Classifications.percent, recordsFWFailedToMatch / recordsCount * 100),
      getFormatted(mapResults, Classifications.recordsFWOverMatched, recordsFWOverMatched),
      getFormatted(mapResults, Classifications.percent, recordsFWOverMatched / recordsCount * 100),
      getFormatted(mapResults, Classifications.recordsFWUnderMatched, recordsFWUnderMatched),
      getFormatted(mapResults, Classifications.percent, recordsFWUnderMatched / recordsCount * 100),
      getFormatted(mapResults, Classifications.recordsInFWOnly, recordsInFWOnly),
      getFormatted(mapResults, Classifications.percent, recordsInFWOnly / recordsCount * 100),
      getFormatted(mapResults, Classifications.recordsInKMOnly, recordsInKMOnly),
      getFormatted(mapResults, Classifications.percent, recordsInKMOnly / recordsCount * 100),
      getFormatted(mapResults, Classifications.recordsFWMismatch, recordsFWMismatch),
      getFormatted(mapResults, Classifications.percent, recordsFWMismatch / recordsCount * 100))
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

  private def getFormatted(mapResults: mutable.HashMap[String, Long], key: String, default: AnyVal) = {

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

  object Classifications {

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
  }

}

//class KeyPartitioner(_numPartitions: Int) extends Partitioner {
//
//    def numPartitions = _numPartitions
//
//    def getPartition(key: Any): Int =
//        math.abs(key.toString().toLowerCase().hashCode()) % numPartitions
//}