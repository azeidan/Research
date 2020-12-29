
package org.cusp.bdi.sb.test

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.cusp.bdi.sb.test
import org.cusp.bdi.sb.test.OutputsCompare.{COL_WIDTH, lstOrdered, recordsBothNoMatch, recordsCorrectMatch, recordsCorrectMatchDistanceOnly, recordsCount, recordsFWOverMatched, recordsFWUnderMatched, recordsInFWOnly, recordsInKMOnly, recordsMismatch}
import org.cusp.bdi.util.Helper

import scala.collection.mutable

object OutputsCompare extends Enumeration with Serializable {

  val recordsBothNoMatch: test.OutputsCompare.Value = Value("Records both no match")
  val recordsCount: test.OutputsCompare.Value = Value("Total Number of Records")
  val recordsCorrectMatch: test.OutputsCompare.Value = Value("Records correctly matched")
  val recordsCorrectMatchDistanceOnly: test.OutputsCompare.Value = Value("Records correctly matched (Dist. only)")
  val recordsMismatch: test.OutputsCompare.Value = Value("Records framework incorrectly matched")
  val recordsFWOverMatched: test.OutputsCompare.Value = Value("Records framework overmatched")
  val recordsFWUnderMatched: test.OutputsCompare.Value = Value("Records framework undermatched")
  val recordsInFWOnly: test.OutputsCompare.Value = Value("Records appeared in framework only")
  val recordsInKMOnly: test.OutputsCompare.Value = Value("Records appeared in key match only")

  val COL_WIDTH: Int = this.values.map(_.toString.length).max

  val lstOrdered = List(recordsCount,
    recordsCorrectMatch,
    recordsCorrectMatchDistanceOnly,
    recordsBothNoMatch,
    recordsFWOverMatched,
    recordsFWUnderMatched,
    recordsInFWOnly,
    recordsInKMOnly,
    recordsMismatch)

  def asMap(): mutable.Map[Value, Double] =
    mutable.HashMap(recordsBothNoMatch -> 0.0,
      recordsCount -> 0.0,
      recordsCorrectMatch -> 0.0,
      recordsCorrectMatchDistanceOnly -> 0.0,
      recordsMismatch -> 0.0,
      recordsFWOverMatched -> 0.0,
      recordsFWUnderMatched -> 0.0,
      recordsInFWOnly -> 0.0,
      recordsInKMOnly -> 0.0)
}

final class OutputsCompare(debugMode: Boolean, classificationCount: Int,
                           rddKeyMatch: RDD[String], keyMatchFileParserClass: String,
                           rddTestFW: RDD[String], testFWFileParserClass: String) extends Serializable {

  val keyMatchFileParser: BenchmarkInputFileParser = instantiateParser(keyMatchFileParserClass)
  val testFWFileParser: BenchmarkInputFileParser = instantiateParser(testFWFileParserClass)

  def compare(): List[String] = {

    val mapClassification = rddKeyMatch
      .mapPartitions(_.map(keyMatchFileParser.parseLine))
      .mapPartitions(_.map(row => (row._1, (row._2, Array[(String, String)]()))))
      .union(
        rddTestFW.mapPartitions(_.map(testFWFileParser.parseLine))
          .mapPartitions(_.map(row => (row._1, (Array[(String, String)](), row._2))))
      )
      .reduceByKey((tpl0, tpl1) => if (tpl0._1.isEmpty) (tpl1._1, tpl0._2) else (tpl0._1, tpl1._2))
      .mapPartitions(iter => {

        val mapClassify = OutputsCompare.asMap()

        def incrementInMap(classificationKey: OutputsCompare.Value, rowKey: String, arrKM: Array[(String, String)], arrFW: Array[(String, String)]): Unit = {

          if (!Array(recordsCount, recordsBothNoMatch, recordsCorrectMatch, recordsCorrectMatchDistanceOnly).contains(classificationKey))
            Helper.loggerSLf4J(debugMode, OutputsCompare, "%n>>\t%s: %s\n\t\t\t>>KM:%s\n\t\t\t>>FW:%s".format(classificationKey, rowKey, arrKM.mkString(","), arrFW.mkString(",")), null)

          mapClassify.update(classificationKey, mapClassify(classificationKey) + 1)
        }

        while (iter.hasNext) {

          val (rowKey, (lstKM, lstFW)) = iter.next

          if (lstKM.nonEmpty || lstFW.nonEmpty)
            incrementInMap(recordsCount, rowKey, lstKM, lstFW)

          if (lstKM.nonEmpty && lstFW.isEmpty) {

            incrementInMap(recordsInKMOnly, rowKey, lstKM, lstFW)
            incrementInMap(recordsMismatch, rowKey, lstKM, lstFW)
          }
          else if (lstKM.isEmpty && lstFW.nonEmpty) {

            incrementInMap(recordsInFWOnly, rowKey, lstKM, lstFW)
            incrementInMap(recordsMismatch, rowKey, lstKM, lstFW)
          }
          else if (lstKM.isEmpty && lstFW.isEmpty) {

            incrementInMap(recordsBothNoMatch, rowKey, lstKM, lstFW)
            incrementInMap(recordsCorrectMatch, rowKey, lstKM, lstFW)
          }
          else if (lstFW.length < classificationCount && lstFW.length < lstKM.length)
            incrementInMap(recordsFWUnderMatched, rowKey, lstKM, lstFW)
          else {

            if (lstKM.length < lstFW.length && lstKM.length < classificationCount)
              incrementInMap(recordsFWOverMatched, rowKey, lstKM, lstFW)

            val lastCompareIndex = Helper.min(classificationCount, lstKM.length) - 1

            val matchCount = (0 to lastCompareIndex)
              .map(idx =>
                // check distance and label except for the last entry due to certain FW discarding extra matches and without sorting
                if (lstKM(idx)._1 == lstFW(idx)._1)
                  if (lstKM(idx)._2.equalsIgnoreCase(lstFW(idx)._2))
                    true
                  else {

                    incrementInMap(recordsCorrectMatchDistanceOnly, rowKey, lstKM, lstFW)
                    true
                  }
                else
                  false
              )
              .take(classificationCount)
              .count(_ == true)

            if (matchCount == lastCompareIndex + 1)
              incrementInMap(recordsCorrectMatch, rowKey, lstKM, lstFW)
            else
              incrementInMap(recordsMismatch, rowKey, lstKM, lstFW)
          }
        }

        mapClassify.iterator
      })
      .reduceByKey(_ + _)
      .collect
      .toMap

    var totalCount = 0.0

    lstOrdered.map(lbl => {

      if (lbl == lstOrdered.head) {

        totalCount = mapClassification(lstOrdered.head)

        ("%" + COL_WIDTH + "s: %,.2f").format(lbl, totalCount)
      } else {

        val lblVal = mapClassification(lbl)
        val percent = lblVal / totalCount * 100
        val percentFormat = if (percent < 0.0001) "e" else "f"

        ("%" + COL_WIDTH + "s: %,.2f%n%" + COL_WIDTH + "s: %.4" + percentFormat + "%%").format(lbl, lblVal, "(%)", percent)
      }
    })
  }

  private def instantiateParser(className: String): BenchmarkInputFileParser = {

    var loadClass = className

    if (className.endsWith("$"))
      loadClass = className.substring(0, className.length() - 1)

    Class.forName(loadClass).getConstructor().newInstance().asInstanceOf[BenchmarkInputFileParser]
  }
}