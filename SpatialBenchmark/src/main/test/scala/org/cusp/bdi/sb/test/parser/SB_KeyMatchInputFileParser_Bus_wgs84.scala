package org.cusp.bdi.sb.test.parser

import org.cusp.bdi.sb.test.BenchmarkInputFileParser

case class SB_KeyMatchInputFileParser_Bus_wgs84() extends BenchmarkInputFileParser {
  def parseLine(line: String): (String, Array[String]) =
    commonParseLine(line, 1, 11)
}
