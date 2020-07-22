package org.cusp.bdi.sb.examples

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

case class SB_KeyMatchInputFileParser_Bus() extends BenchmarkInputFileParser {
    def parseLine(line: String): (String, Array[String]) =
        commonParseLine(line, 0, 11)
}

case class SB_KeyMatchInputFileParser_Taxi() extends BenchmarkInputFileParser {
    def parseLine(line: String): (String, Array[String]) =
        commonParseLine(line, 0, 18)
}

case class SB_KeyMatchInputFileParser_TPEP_wgs84() extends BenchmarkInputFileParser {
    def parseLine(line: String): (String, Array[String]) =
        commonParseLine(line, 2, 5)
}

case class SB_KeyMatchInputFileParser_Taxi_wgs84() extends BenchmarkInputFileParser {
    def parseLine(line: String): (String, Array[String]) =
        commonParseLine(line, 5, 18)
}

case class SB_KeyMatchInputFileParser_Bus_wgs84() extends BenchmarkInputFileParser {
    def parseLine(line: String): (String, Array[String]) =
        commonParseLine(line, 1, 11)
}

case class SB_KeyMatchInputFileParser_TPEP() extends BenchmarkInputFileParser {
    def parseLine(line: String): (String, Array[String]) =
        commonParseLine(line, 0, 5)
}

case class SB_KeyMatchInputFileParser_RandomPoints() extends BenchmarkInputFileParser {

    def parseLine(line: String): (String, Array[String]) = {

        val arr = line.split(';')

        (arr(0), arr.slice(1, arr.length))
    }
}

//case class SB_KeyMatchInputFileParser_RandomPoints() extends SB_KeyMatchInputFileParser_RandomPoints_Common with BenchmarkInputFileParser {
//
//    def parseLine(line: String) =
//        super.parseLine(line, false)
//}

//case class SB_KeyMatchInputFileParser_RandomPoints_RemoveFirstMatch() extends SB_KeyMatchInputFileParser_RandomPoints_Common with BenchmarkInputFileParser {
//
//    def parseLine(line: String) =
//        super.parseLine(line, true)
//}

case class SB_KeyMatchInputFileParser_Bus_Small() extends BenchmarkInputFileParser {

    def parseLine(line: String): (String, Array[String]) = {

        val arr = line.split(';')

        (arr(0), arr.slice(1, arr.length).map(x => x.substring(x.indexOf(',') + 1)))
    }
}

case class KM_InputFileParser_OSM_Buildings() extends BenchmarkInputFileParser {
    def parseLine(line: String): (String, Array[String]) = {

        val idx = Helper.indexOf(line, "))")

        if (idx + 3 == line.length)
            (line, null)
        else
            (line.substring(0, idx + 3), line.substring(idx + 4).split(','))
    }
}