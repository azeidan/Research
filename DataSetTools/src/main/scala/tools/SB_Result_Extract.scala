package tools

import java.io.File
import java.nio.file.Files
import java.nio.file.Paths

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.SortedSet

object SB_Result_Extract {
  def main(args: Array[String]): Unit = {

//    val sSet = SortedSet[(String, String)]()

    new File("/media/cusp/Data/GeoMatch_Files/SpatialBenchmark/")
      .listFiles()
      .filter(!_.isDirectory())
      .filter(_.getName.endsWith(".txt"))
      .filter(_.length() > 0)
      .map(txtFile => {

        val key = txtFile.getName

        val arr = Files.lines(Paths.get(txtFile.getAbsolutePath))
          .toArray()
          .filter(!_.toString().startsWith("Total Runtime"))
          .map(_.toString().split(": "))
          .map(_.map(_.replaceAll("\\s+\\W", "")))

        val lst = ListBuffer((arr(0)(0), arr(0)(1), ""))

        (1 until arr.length by 2).foreach(i =>
          lst.append((arr(i)(0), arr(i)(1), arr(i + 1)(1))))

        lst.map(row => (key.substring(0, key.length() - 4), row))
      })
      .flatMap(_.iterator)
      .foreach(row => printf("%s\t%s:\t%s\t%s%n", row._1, row._2._1, row._2._2, row._2._3))

    //                    .filter(x => true)

    //                val gzFilePath = new File(dir.getAbsoluteFile + File.separator + "part-00000.gz").getAbsolutePath
    //
    //                var in = new GZIPInputStream(new FileInputStream(gzFilePath))
    //
    //                val buf = new Array[Byte](1024)
    //
    //                var n = in.read(buf)
    //
    //                val sbLine = StringBuilder.newBuilder
    //
    //                while (n > 0) {
    //
    //                    sbLine.append(new String(buf, 0, n))
    //
    //                    n = in.read(buf)
    //                }
    //
    //                in.close()
    //
    //                sbLine.split(10.toChar)
    //                    .filter(!_.contains("%"))
    //                    .filter(!_.contains("Runtime:"))
    //                //                    .foreach(line => ("%s%n", line.replaceAll("\\s+\\W", "").replace(": ", ":\t")))
    //
    //                sSet.add(key, sbLine.split(10.toChar)
    //                    .filter(!_.contains("%"))
    //                    .filter(!_.contains("Runtime:"))
    //                    .map(line => line.replaceAll("\\s+\\W", "").replace(": ", ":\t"))
    //                    .mkString("\n"))
    //
    //        sSet.foreach(kv => printf("%s%s%n", kv._1, kv._2))
  }
}