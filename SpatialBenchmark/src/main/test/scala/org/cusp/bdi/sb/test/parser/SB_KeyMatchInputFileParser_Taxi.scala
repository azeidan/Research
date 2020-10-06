package org.cusp.bdi.sb.test.parser

import org.cusp.bdi.sb.test.BenchmarkInputFileParser

case class SB_KeyMatchInputFileParser_Taxi() extends BenchmarkInputFileParser {
  def parseLine(line: String): (String, Array[String]) =
    commonParseLine(line, 0, 18)
}
