package org.cusp.bdi.sb.test.parser

import org.cusp.bdi.sb.test.BenchmarkInputFileParser

case class SB_KeyMatchInputFileParser_RandomPoints() extends BenchmarkInputFileParser {

  override def parseLine(line: String): (String, Array[(String, String)]) =
    commonParseLine(line, ';', ',')
}
