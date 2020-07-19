package org.cusp.bdi.tools

import java.io.BufferedOutputStream
import java.io.FileOutputStream
import scala.io.Source
import java.io.BufferedWriter
import java.io.FileWriter

object EnumerateFile {

    def main(args: Array[String]): Unit = {

        val dir = "/media/ayman/Data/GeoMatch_Files/InputFiles/RandomSamples/"

        val idx0 = 0
        val idx1 = 3
        val idx2 = 4

        Array("Bread_1_A.csv.del",
            "Bread_1_B.csv.del",
            "Bread_2_A.csv.del",
            "Bread_2_B.csv.del",
            "Bread_3_A.csv.del",
            "Bread_3_B.csv.del")
            .foreach(fileName => {

                val output = new BufferedWriter(new FileWriter(dir + fileName.substring(0, fileName.length() - 4)))

                Source.fromFile(dir + fileName).getLines().foreach(line => {

                    val arr = line.split(",")

                    output.write(arr(idx0))
                    output.write(',')
                    output.write(arr(idx1))
                    output.write(',')
                    output.write(arr(idx2))
                    output.write('\n')
                })

                output.flush()
                output.close()
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