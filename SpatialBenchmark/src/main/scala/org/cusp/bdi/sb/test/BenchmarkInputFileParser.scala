package org.cusp.bdi.sb.test

import org.cusp.bdi.util.Helper

trait BenchmarkInputFileParser extends Serializable {

  def parseLine(line: String): (String, Array[String])

  // "matchNthComma" is the number of the comma separating the record from the street matches
  // e.g. matchNthComma=11, then the 11th comma is the one separating the point from the street matches
  protected def commonParseLine: (String, Int, Int) => (String, Array[String]) = (line: String, latLonNthComma: Int, matchNthComma: Int) => {

    var arrStreetMatches: Array[String] = null

    val idxStreet = Helper.indexOf(line, ",", matchNthComma)
    val recordLine = StringBuilder.newBuilder.append(line)

    if (idxStreet != -1) {

      arrStreetMatches = line.substring(idxStreet + 1).split(',')
      recordLine.delete(idxStreet, recordLine.length)
    }

    val idxCoord = Helper.indexOf(recordLine, ",", latLonNthComma)

    if (idxCoord != -1) {

      // remove lon/lat from line
      recordLine.delete(idxCoord, Helper.indexOf(recordLine, ",", 2, idxCoord + 1))

    }

    (recordLine.toString(), arrStreetMatches)
  }
}
