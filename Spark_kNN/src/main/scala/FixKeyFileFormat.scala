import scala.io.Source
import java.io.PrintWriter
import java.io.File
import java.nio.file.Files

object FixKeyFileFormat {

    def main(args: Array[String]): Unit = {

        //        val writer = new PrintWriter(new File("/media/cusp/Data/GeoMatch_Files/InputFiles/Bus_TripRecod_NAD83_part-00000_SMALL_key_SORTED_2.csv"))
        //
        //        Source.fromFile("/media/cusp/Data/GeoMatch_Files/InputFiles/Bus_TripRecod_NAD83_part-00000_SMALL_key_SORTED.csv")
        //            .getLines
        //            .foreach(line => {
        //
        //                val arr = line.substring(0, line.length() - 2).split(",\\[")
        //
        //                val arr2 = arr(1).substring(1, arr(1).length - 1).split("\\),\\(")
        //
        //                writer.write(StringBuilder.newBuilder.append(arr(0))
        //                    .append(';')
        //                    .append(arr2.mkString(";"))
        //                    .append(System.lineSeparator())
        //                    .toString)
        //            })

        Source.fromFile("/media/cusp/Data/GeoMatch_Files/InputFiles/Bus_TripRecod_NAD83_part-00000_SMALL_key_SORTED.csv")
            .getLines
            .foreach(line => {

                val arr = line.substring(0, line.length() - 2).split(",\\[")

//                val arr2 = arr(1).substring(1, arr(1).length - 1).split("\\),\\(")

//                writer.write(StringBuilder.newBuilder.append(arr(0))
//                    .append(';')
//                    .append(arr2.mkString(";"))
//                    .append(System.lineSeparator())
//                    .toString)
            })

    }
}