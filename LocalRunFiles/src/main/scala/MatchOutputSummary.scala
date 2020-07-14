import scala.io.Source
import scala.collection.mutable.ListBuffer

object MatchOutputSummary {

    def main(args: Array[String]): Unit = {

        val map = Source.fromFile("/media/ayman/Data/GeoMatch_Files/OutputFiles/randomPoints_with_k.txt")
            .getLines()
            .map(line => {

                val arr = line.substring(2, line.length()).split('\t')

                (arr(0).toLowerCase(), arr(1))
            })
            .toMap

        val iterLines = Source.fromFile("/media/ayman/Data/GeoMatch_Files/OutputFiles/tbd1.txt")
            .getLines()

        while (iterLines.hasNext) {
            val line1 = iterLines.next()
            val line2 = iterLines.next()
            val line3 = iterLines.next()

            //            if (line1.startsWith(">>rb_9150"))
            //                println

            val arrMatches1 = line2.substring(2, line2.length() - 1).split("\\),\\(").map(_.split(','))
            val arrMatches2 = line3.substring(2, line3.length() - 1).split("\\),\\(").map(_.split(','))
            var idx2 = 0
            var sb = StringBuilder.newBuilder

            var idx1 = 0
            //            if (line1.startsWith(">>ra_829493"))
            //                println()
            while (idx2 < arrMatches2.length) {

                if (arrMatches1(idx1)(0).equalsIgnoreCase(arrMatches2(idx2)(0)) && arrMatches1(idx1)(1).equalsIgnoreCase(arrMatches2(idx2)(1)))
                    idx1 += 1
                else {

                    var key1 = line1.substring(2, line1.indexOf(','))

                    if (key1.endsWith(")"))
                        key1 = key1.substring(0, key1.length - 1)

                    var key2 = arrMatches2(idx2)(1)

                    if (key2.endsWith(")"))
                        key2 = key2.substring(0, key2.length - 1)

                    val part1 = map.get(key1.toLowerCase()).get
                    val part2 = map.get(key2.toLowerCase()).get

                    if (part1.equals(part2))
                        println("%s %s %s %s".format(key1, part1, key2, part2))
                }

                idx2 += 1
            }
        }
    }
}