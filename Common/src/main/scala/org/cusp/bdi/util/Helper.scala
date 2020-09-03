/**
 * Copyright (c) 2019, The City University of New York and the University of Helsinki
 * All rights reserved.
 */

package org.cusp.bdi.util

import java.io.File

import scala.collection.mutable.ListBuffer
import scala.util.Random

object Helper {

  def isNullOrEmpty(arr: Array[_]) =
    arr == null || arr.isEmpty

  def isNullOrEmpty(list: ListBuffer[_]) =
    list == null || list.isEmpty

  def isNullOrEmpty(str: String) =
    str == null || str.isEmpty

  def isNullOrEmpty(str: Object*) =
    if (str == null)
      true
    else
      str.count(x => x == null || x.toString.length == 0) == str.length

  /**
   * Returns true is the parameter represents a "True" value as defined in LST_BOOL_VALS, false otherwise
   */
  def isBooleanStr(objBool: Object) =
    toBoolean(objBool.toString)

  def toBoolean(strBool: String) =
    strBool.charAt(0).toUpper match {
      case 'T' | 'Y' => true
      case _ => false
    }

  def floorNumStr(str: String) = {

    var idx = 0

    while (idx < str.length() && str.charAt(idx) != '.') idx += 1

    str.substring(0, idx)
  }

  def indexOf(str: String, searchStr: String): Int =
    indexOf(str, searchStr, 1, 0)

  def indexOf(str: String, searchStr: String, n: Int, startIdx: Int) =
    indexOfCommon(StringLikeObj(str, null), searchStr, n, startIdx)

  def indexOf(str: String, searchStr: String, nth: Int): Int =
    indexOfCommon(StringLikeObj(str, null), searchStr, nth, 0)

  def indexOf(strBuild: StringBuilder, searchStr: String, nth: Int): Int =
    indexOfCommon(StringLikeObj(null, strBuild), searchStr, nth, 0)

  def indexOf(strBuild: StringBuilder, searchStr: String, n: Int, startIdx: Int) =
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
    def indexOf(str: String, fromIndex: Int) = {
      if (strBuildObj != null)
        strBuildObj.indexOf(str, fromIndex)
      else
        stringObj.indexOf(str, fromIndex)
    }
  }

  //  def getMBREnds(arrCoords: Array[(Int, Int)], expandBy: Int) = {
  //
  //    val xCoords = arrCoords.map(_._1)
  //    val yCoords = arrCoords.map(_._2)
  //
  //    var minX = xCoords.min - expandBy
  //    var minY = yCoords.min - expandBy
  //    var maxX = xCoords.max + expandBy
  //    var maxY = yCoords.max + expandBy
  //
  //    Array((minX, minY), (maxX, maxY))
  //  }
  //
  //  def getMBREnds(arrCoords: Array[(Float, Float)], expandBy: Float) = {
  //
  //    val xCoords = arrCoords.map(_._1)
  //    val yCoords = arrCoords.map(_._2)
  //
  //    var minX = xCoords.min - expandBy
  //    var minY = yCoords.min - expandBy
  //    var maxX = xCoords.max + expandBy
  //    var maxY = yCoords.max + expandBy
  //
  //    // Closed ring MBR (1st and last points repeated)
  //    Array((minX, minY), (maxX, maxY))
  //  }

  def getMBREnds(arrCoords: Array[(Double, Double)], expandBy: Double) = {

    val xCoords = arrCoords.map(_._1)
    val yCoords = arrCoords.map(_._2)

    // Closed ring MBR (1st and last points repeated)
    Array((xCoords.min - expandBy, yCoords.min - expandBy), (xCoords.max + expandBy, yCoords.max + expandBy))
  }

  //  def getMBR_ClosedRing(arrCoords: Array[(Double, Double)], expandBy: Double) = {
  //
  //    val xCoords = arrCoords.map(_._1)
  //    val yCoords = arrCoords.map(_._2)
  //
  //    var minX = xCoords.min - expandBy
  //    var maxX = xCoords.max + expandBy
  //    var minY = yCoords.min - expandBy
  //    var maxY = yCoords.max + expandBy
  //
  //    // Closed ring MBR (1st and last points repeated)
  //    Array((minX, minY), (maxX, minY), (maxX, maxY), (minX, maxY), (minX, minY))
  //  }
  //
  //  def compareToPrecision(x: Double, y: Double) = {
  //
  //    val diff = x - y
  //
  //    if (math.abs(diff) < 0.000001)
  //      0
  //    else
  //      diff
  //  }

  /**
   * Sends message(s) to the log belonging to the class when debug is turned on
   */
  //  def logMessage(debugOn: Boolean, clazz: => Any, message: => Object) {
  //    if (debugOn)
  //      Logger.getLogger(clazz.getClass().getName).info("# " + message)
  //  }

  /**
   * Randomizes the output directory. This is used when running Spark in local mode for testing
   */
  def randOutputDir(outputDir: String) = {

    val randOutDir = StringBuilder.newBuilder
      .append(outputDir)

    if (!outputDir.endsWith(File.separator))
      randOutDir.append(File.separator)

    randOutDir.append(Random.nextInt(999))

    randOutDir.toString()
  }

  //  def delDirHDFS(sparkContext: SparkContext, dirPath: String) {
  //
  //    try {
  //      val hdfs = FileSystem.get(sparkContext.hadoopConfiguration)
  //      val path = new Path(dirPath)
  //      if (hdfs.exists(path))
  //        hdfs.delete(path, true)
  //    }
  //    catch {
  //      case ex: Exception => ex.printStackTrace()
  //    }
  //  }
  //
  //  def delDirHDFS(dirPath: String) {
  //
  //    val directory = new Directory(new File(dirPath))
  //    directory.deleteRecursively()
  //  }
  //
  //  def ensureClosedRingCoordinates[T <: AnyVal](arrCoord: Array[(T, T)]) = {
  //
  //    var retArr = arrCoord
  //
  //    while (retArr.length < 4 || retArr.head._1 != retArr.last._1 || retArr.head._2 != retArr.last._2)
  //      retArr = retArr :+ arrCoord.head
  //
  //    retArr
  //  }
  //
  //  def euclideanDist(xy1: (Double, Double), xy2: (Double, Double)) =
  //    math.sqrt(math.pow(xy1._1 - xy2._1, 2) + math.pow(xy1._2 - xy2._2, 2))
  //
  //
  //  def manhattanDist(x1: Double, y1: Double, x2: Double, y2: Double) =
  //    math.abs(x1 - x2) + math.abs(y1 - y2)

  def squaredDist(x1: Double, y1: Double, x2: Double, y2: Double) =
    math.pow(x1 - x2, 2) + math.pow(y1 - y2, 2)

  def toByte(str: String) = {
    var idx = 0
    while (idx < str.length() && Character.isDigit(str.charAt(idx))) idx += 1

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