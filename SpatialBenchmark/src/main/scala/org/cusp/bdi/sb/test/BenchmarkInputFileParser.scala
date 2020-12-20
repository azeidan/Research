package org.cusp.bdi.sb.test

import org.cusp.bdi.sb.test.BenchmarkInputFileParser.sortSimilarByKey

object BenchmarkInputFileParser {

  def sortSimilarByKey(arrMatches: Array[(String, String)]): Array[(String, String)] = {

    var arr = arrMatches

    var i = 0
    while (i < arr.length - 1) {

      val sortFrom = i

      while (i < arr.length - 1 && arr(sortFrom)._1.equals(arr(i + 1)._1))
        i += 1

      if (sortFrom != i) {

        val arrPart1 = if (sortFrom == 0) Array[(String, String)]() else arr.slice(0, sortFrom)
        val arrPart2 = arr.slice(sortFrom, i + 1).sortBy(_._2)
        val arrPart3 = if (i == arr.length - 1) Array[(String, String)]() else arr.slice(i + 1, arr.length)

        arr = arrPart1 ++ arrPart2 ++ arrPart3
      }

      i += 1
    }

    arr
  }
}

trait BenchmarkInputFileParser extends Serializable {

  def parseLine(line: String): (String, Array[(String, String)])

  // assumes line is in the following format:
  // <key> <delimitChar> <delimitCharMajor> <delimitChar> <match2> <delimitCharMajor> <match3> <delimitCharMajor> ...
  //
  // key is assumed in the following format
  // <label> <delimitCharMinor> <val1> <delimitCharMinor> <val2>
  //
  // each match is assumed in the following format
  // <value> <delimitCharMinor> <key>
  protected def commonParseLine(line: String, delimitCharMajor: Char, delimitCharMinor: Char): (String, Array[(String, String)]) = {

    val arr = line.toLowerCase.split(delimitCharMajor)

    val arrMatches =
      sortSimilarByKey(arr
        .tail
        .map(_.split(delimitCharMinor))
        .map(arrDistKey => (arrDistKey(0), arrDistKey(1)))
      )
    // sort similar values by key
    (arr(0), arrMatches)
  }
}
