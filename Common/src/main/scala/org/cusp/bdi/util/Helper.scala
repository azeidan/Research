/**
 * Copyright (c) 2019, The City University of New York and the University of Helsinki
 * All rights reserved.
 */

package org.cusp.bdi.util

import java.io.File
import java.util.logging.Logger

import scala.collection.mutable.ListBuffer
import scala.util.Random

object Helper {

  def max(x: Int, y: Int): Int = if (x > y) x else y

  def max(x: Long, y: Long): Long = if (x > y) x else y

  def max(x: Double, y: Double): Double = if (x > y) x else y

  def min(x: Int, y: Int): Int = if (x < y) x else y

  def min(x: Double, y: Double): Double = if (x < y) x else y

  def min(x: Long, y: Long): Long = if (x < y) x else y

  def log2(n: Long): Int = (math.log(n) / math.log(2)).toInt

  def indexOfBSearch(arrSortedValues: IndexedSeq[Int], lookupValue: Int): Boolean = {

    if (lookupValue >= arrSortedValues.head && lookupValue <= arrSortedValues.last) {

      var lowerIdx = 0
      var upperIdx = arrSortedValues.length - 1

      while (lowerIdx <= upperIdx) {

        val midIdx = lowerIdx + (upperIdx - lowerIdx) / 2

        if (arrSortedValues(midIdx) == lookupValue)
          return true
        else if (arrSortedValues(midIdx) > lookupValue)
          upperIdx = midIdx - 1
        else
          lowerIdx = midIdx + 1
      }
    }

    false
  }

  def toString(bool: Boolean): String =
    if (bool) "T" else "F"

  def isNullOrEmpty(arr: Array[_]): Boolean =
    arr == null || arr.isEmpty

  def isNullOrEmpty(list: ListBuffer[_]): Boolean =
    list == null || list.isEmpty

  def isNullOrEmpty(str: String): Boolean =
    str == null || str.isEmpty

  def isNullOrEmpty(str: Object*): Boolean =
    if (str == null)
      true
    else
      str.count(x => x == null || x.toString.isEmpty) == str.length

  def isBooleanStr(objBool: Object): Boolean =
    toBoolean(objBool.toString)

  def toBoolean(strBool: String): Boolean =
    strBool.charAt(0).toUpper match {
      case 'T' | 'Y' => true
      case _ => false
    }

  def floorNumStr(str: String): String = {

    var idx = 0

    while (idx < str.length() && str.charAt(idx) != '.') idx += 1

    str.substring(0, idx)
  }

  def indexOf(str: String, searchStr: String): Int =
    indexOf(str, searchStr, 1, 0)

  def indexOf(str: String, searchStr: String, n: Int, startIdx: Int): Int =
    indexOfCommon(StringLikeObj(str, null), searchStr, n, startIdx)

  def indexOf(str: String, searchStr: String, nth: Int): Int =
    indexOfCommon(StringLikeObj(str, null), searchStr, nth, 0)

  def indexOf(strBuild: StringBuilder, searchStr: String, nth: Int): Int =
    indexOfCommon(StringLikeObj(null, strBuild), searchStr, nth, 0)

  def indexOf(strBuild: StringBuilder, searchStr: String, n: Int, startIdx: Int): Int =
    indexOfCommon(StringLikeObj(null, strBuild), searchStr, n, startIdx)

  /**
   * Returns the nth index of, or -1 if out of range
   */
  private def indexOfCommon(strOrSBuild: StringLikeObj, searchStr: String, n: Int, startIdx: Int) = {

    var idx = startIdx - 1
    var break = false
    var i = 0

    while (i < n && !break) {

      idx = strOrSBuild.indexOf(searchStr, idx + 1)

      if (idx == -1)
        break = true
      i += 1
    }

    idx
  }

  case class StringLikeObj(stringObj: String, strBuildObj: StringBuilder) {
    def indexOf(str: String, fromIndex: Int): Int = {
      if (strBuildObj != null)
        strBuildObj.indexOf(str, fromIndex)
      else
        stringObj.indexOf(str, fromIndex)
    }
  }

  def getMBREnds(arrCoords: Array[(Double, Double)], expandBy: Double): Array[(Double, Double)] = {

    val xCoords = arrCoords.map(_._1)
    val yCoords = arrCoords.map(_._2)

    // Closed ring MBR (1st and last points repeated)
    Array((xCoords.min - expandBy, yCoords.min - expandBy), (xCoords.max + expandBy, yCoords.max + expandBy))
  }

  /**
   * Sends message(s) to the log belonging to the class when debug is turned on
   */
  def loggerSLf4J(debugOn: Boolean, clazz: => Any, message: => Object) {
    if (debugOn)
      Logger.getLogger(clazz.getClass.getName).info(message.toString)
  }

  /**
   * Randomizes the output directory. This is used when running Spark in local mode for testing
   */
  def randOutputDir(outputDir: String): String = {

    val randOutDir = StringBuilder.newBuilder
      .append(outputDir)

    if (!outputDir.endsWith(File.separator))
      randOutDir.append(File.separator)

    randOutDir.append(Random.nextInt(999))

    randOutDir.toString()
  }

  def manhattanDist(x1: Double, y1: Double, x2: Double, y2: Double): Int =
    Helper.max(math.abs(x1 - x2), math.abs(y1 - y2)).ceil.toInt

  def squaredEuclideanDist(x1: Double, y1: Double, x2: Double, y2: Double): Double =
    math.pow(x1 - x2, 2) + math.pow(y1 - y2, 2)

  def euclideanDist(x1: Double, y1: Double, x2: Double, y2: Double): Double =
    math.sqrt(squaredEuclideanDist(x1, y1, x2, y2))

  def toByte(str: String): Long = {

    var idx = 0

    while (idx < str.length() && Character.isDigit(str.charAt(idx))) idx += 1

    if (idx == str.length)
      str.toInt
    else {
      val unit = str.charAt(idx).toUpper
      var byteVal = str.substring(0, idx).toLong

      if (!unit.equals('B')) {

        byteVal *= 1000
        if (!unit.equals('K')) {

          byteVal *= 1000
          if (!unit.equals('M'))
            byteVal *= 1000
        }
      }

      byteVal
    }
  }

}