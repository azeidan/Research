package tools

import java.nio.file.{Files, Paths}

import scala.io.Source

object LogCleaner {

  def main(args: Array[String]): Unit = {

    //    val bw = Files.newBufferedWriter(Paths.get("/media/ayman/Data/GeoMatch_Files/InputFiles/tbd4.txt"))
    //
    //    Source.fromFile("/media/ayman/Data/GeoMatch_Files/InputFiles/tbd2.txt")
    //      .getLines()
    //      .foreach(line => {
    //        bw.write(line.split("\\)\t").tail.mkString("\t"))
    //        bw.write("\n")
    //      })
    //    bw.flush()
    //    bw.close()

 val l =    Source.fromFile("/media/ayman/Data/GeoMatch_Files/InputFiles/tbd4.txt")
      .getLines()
      .map(line => {

        val arr = line.split("\t")

        (arr(0), Byte.MinValue)
      }).toList.groupBy(_._1).map(x=>(x._1, x._2.size))

    val ll =    Source.fromFile("/media/ayman/Data/GeoMatch_Files/InputFiles/tbd4.txt")
      .getLines()
      .map(line => {

        val arr = line.split("\t")

        (arr(1), Byte.MinValue)
      }).toList.groupBy(_._1).map(x=>(x._1, x._2.size))

    println(l)
    println(ll)
  }
}
