package org.cusp.bdi.sb.test.parser

import org.cusp.bdi.sb.test.BenchmarkInputFileParser

case class SB_KeyMatchInputFileParser_Bus_Small() extends BenchmarkInputFileParser {

  def parseLine(line: String): (String, Array[String]) = {

    val arr = line.split(';')

    (arr(0), arr.slice(1, arr.length).map(x => x.substring(x.indexOf(',') + 1)))
  }
}
