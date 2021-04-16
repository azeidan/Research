package org.cusp.bdi.util

import scala.collection.mutable.ListBuffer

object InputFileParsers extends Serializable {

  val CODE_TPEP_POINT = "TPEP_Point"
  val CODE_TPEP_POINT_WGS84 = "TPEP_Point_WGS84"
  val CODE_OSM_POINT_WGS84 = "OSM_Point_WGS84"
  val CODE_TAXI_POINT = "Taxi_Point"
  val CODE_THREE_PART_LINE = "Three_Part_Line"
  val CODE_POI_NYC = "POI_NYC"
  val CODE_BUS_POINT = "Bus_Point"
  val CODE_BUS_POINT_SHIFTED = "Bus_Point_shifted"
  val CODE_RAND_POINT = "Rand_Point"

  def getLineParser(objType: String): String => (String, (String, String)) =
    objType match {
      case s if s matches "(?i)" + CODE_TPEP_POINT_WGS84 => InputFileParsers.tpepPoints_WGS84
      case s if s matches "(?i)" + CODE_OSM_POINT_WGS84 => InputFileParsers.osmPoints_WGS84
      case s if s matches "(?i)" + CODE_TPEP_POINT => InputFileParsers.tpepPoints
      case s if s matches "(?i)" + CODE_TAXI_POINT => InputFileParsers.taxiPoints
      case s if s matches "(?i)" + CODE_THREE_PART_LINE => InputFileParsers.threePartLine
      case s if s matches "(?i)" + CODE_BUS_POINT => InputFileParsers.busPoints
      case s if s matches "(?i)" + CODE_BUS_POINT_SHIFTED => InputFileParsers.busPoints
      case s if s matches "(?i)" + CODE_RAND_POINT => InputFileParsers.randPoints
      case s if s matches "(?i)" + CODE_POI_NYC => InputFileParsers.poiNYC
    }

  def nycLION: String => (String, Array[(String, String)]) = (line: String) => {

    try {
      val idx0 = line.indexOf(',')
      val streetID = line.substring(0, idx0)

      val lineCoords = line.substring(idx0 + 20, line.length() - 3)

      val coordArr = lineCoords.split(',')
        .map(row => {

          val arr = row.split(' ')

          (arr(0), arr(1))
        })

      (streetID, coordArr)
    }
    catch {
      case ex: Exception =>

        ex.printStackTrace()
        null
    }
  }

  def nycLION_WGS84: String => (String, Array[(String, String)]) = (line: String) =>
    nycLION(line)

  def nycLION_Segments: String => Array[(String, ((String, String), (String, String)))] = (line: String) => {

    val (streetID, coordArr) = nycLION(line)

    if (streetID == null)
      null
    else {

      val lst = ListBuffer[(String, ((String, String), (String, String)))]()

      var startPoint = coordArr(0)

      (1 until coordArr.length).foreach(i => {

        val endPoint = coordArr(i)

        lst += ((streetID, (startPoint, endPoint)))

        startPoint = endPoint
      })

      lst.toArray
    }
  }

  def tpepPoints: String => (String, (String, String)) = (line: String) => {

    val xy = getXY(line, 2 /*, false*/)

    if (xy == null)
      null
    else
      (line.toLowerCase, xy)
  }

  def tpepPoints_WGS84: String => (String, (String, String)) = (line: String) => {

    val xy = getXY(line, 2 /*, false*/)

    if (xy == null)
      null
    else
      (line.toLowerCase, xy)
  }

  def taxiPoints: String => (String, (String, String)) = (line: String) => {

    val xy = getXY(line, 5)

    if (xy == null || xy._1.contains('-') || xy._2.contains('-'))
      null
    else
      (line.toLowerCase, xy)
  }

  def threePartLine: String => (String, (String, String)) = (line: String) => {

    val parts = line.split(",")

    if (parts(1).charAt(0) == '-' || parts(2).charAt(0) == '-')
      null
    else
      (parts(0), (parts(1), parts(2)))
  }

  def taxiPoints_WGS84: String => (String, (String, String)) = (line: String) => {

    val xy = getXY(line, 5 /*, false*/)

    if (xy == null)
      null
    else
      (line.toLowerCase, xy)
  }

  def busPoints: String => (String, (String, String)) = (line: String) => {

    val xy = getXY(line, 1 /*, true*/)

    if (xy == null)
      null
    else
      (line.toLowerCase, xy)
  }

  def busPoints_WGS84: String => (String, (String, String)) = (line: String) => {

    val xy = getXY(line, 1)

    if (xy == null)
      null
    else
      (line, (xy._2, xy._1)) // lon/lat reversed in data source
  }

  def poiNYC: String => (String, (String, String)) = (line: String) =>
    if (line.length == 0)
      null
    else {

      val xy = getXY(line, 10)

      if (xy == null)
        null
      else
        (line, (xy._1, xy._2)) // lon/lat reversed in data source
    }

  def osmPoints_WGS84: String => (String, (String, String)) = (line: String) =>
    getXY_NoLoad(line)

  def randPoints: String => (String, (String, String)) = (line: String) => {

    val xy = line.split(',')

    (xy(0), (xy(1), xy(2)))
  }

  private def getXY_NoLoad(line: String) =
    try {

      val xy = line.split(',')

      if (xy.length != 2)
        null
      else
        (line, (xy.head, xy.last))
    }
    catch {
      case ex: Exception =>

        ex.printStackTrace()
        null
    }

  private def getXY(line: String, startCommaNum: Int) = {

    def getCommaPos(commaNum: Int, startIdx: Int) = {

      var count = 0
      var idx = startIdx

      while (count < commaNum) {

        if (line(idx) == ',')
          count += 1

        idx += 1
      }

      idx
    }

    try {

      val idx0 = getCommaPos(startCommaNum, 0)
      val idx1 = getCommaPos(1, idx0 + 1)
      val idx2 = getCommaPos(1, idx1 + 1)

      val x = line.substring(idx0, idx1 - 1)
      val y = line.substring(idx1, idx2 - 1)

      if (x(0) != '-' && (x(0) < '0' || x(0) > '9'))
        null
      else
        (x, y)
    }
    catch {
      case ex: Exception =>

        ex.printStackTrace()
        null
    }
  }
}