package org.cusp.bdi.sb.test.parser

import org.cusp.bdi.sb.test.BenchmarkInputFileParser
import org.cusp.bdi.util.Helper

case class SB_KeyMatchInputFileParser_OSM_Buildings() extends BenchmarkInputFileParser {
  def parseLine(line: String): (String, Array[String]) = {

    val idx = Helper.indexOf(line, "))")

    if (idx + 3 == line.length)
      (line, null)
    else
      (line.substring(0, idx + 3), line.substring(idx + 4).split(','))
  }
}
