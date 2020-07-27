package org.cusp.bdi.tools

import java.io.BufferedOutputStream
import java.io.FileOutputStream

import scala.io.Source
import java.io.BufferedWriter
import java.io.FileWriter

import scala.io

object EnumerateFile {

  def main(args: Array[String]): Unit = {

    val dir = "/media/ayman/Data/GeoMatch_Files/InputFiles/RandomSamples/Original/"

    Array(("Bread", 2), ("Bus", 1), ("Taxi", 5))
      .foreach(row => {

        val (file, start) = row

        Range(1, 4).foreach(i => {

          var counter = 0

          val output1 = new BufferedWriter(new FileWriter(dir + "../" + file + "_" + i + "_A.csv"))
          val output2 = new BufferedWriter(new FileWriter(dir + "../" + file + "_" + i + "_B.csv"))

          Source.fromFile(dir + "/" + file + i + ".csv").getLines().foreach(line => {

            val group = if (counter < 1e6) 'A' else 'B'
            val output = if (counter < 1e6) output1 else output2
            val subtract = if (counter < 1e6) 0 else 1e6.toInt

            val arr = line.split(",")

            output.write(file + "_" + i + "_" + group + "_" + (counter - subtract) + ",")

            (start to start + 1).foreach(idx => {

              output.write(arr(idx))

              if (idx == start + 1)
                output.write('\n')
              else
                output.write(',')
            })

            counter += 1
          })

          output1.flush()
          output1.close()
          output2.flush()
          output2.close()
        })
      })

    //        val dir = "/media/ayman/Data/GeoMatch_Files/InputFiles/RandomSamples/"
    //
    //        Array("Bread_1.csv",
    //            "Bread_2.csv",
    //            "Bread_3.csv",
    //            "Bus_1.csv",
    //            "Bus_2.csv",
    //            "Bus_3.csv",
    //            "Yellow_1.csv",
    //            "Yellow_2.csv",
    //            "Yellow_3.csv")
    //            .foreach(fileName => {
    //
    //                println("Processing: " + fileName)
    //
    //                val name = fileName.substring(0, fileName.length() - 4)
    //
    //                val output1 = new BufferedWriter(new FileWriter(dir + name + "_A" + ".csv"))
    //                val output2 = new BufferedWriter(new FileWriter(dir + name + "_B" + ".csv"))
    //
    //                var counter = 0
    //
    //                Source.fromFile(dir + fileName).getLines().foreach(line => {
    //
    //                    output1.write(name + "_A_" + counter + "," + line + "\n")
    //                    output2.write(name + "_B_" + counter + "," + line + "\n")
    //
    //                    counter += 1
    //                })
    //
    //                output1.flush()
    //                output2.flush()
    //
    //                output1.close()
    //                output2.close()
    //
    //                println("  Finished: " + fileName)
    //            })
  }
}