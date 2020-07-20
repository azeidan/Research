package org.cusp.bdi.tools

import scala.io.Source

object ExtractRecords {

  private val inputile_A = "/media/cusp/Data/GeoMatch_Files/InputFiles/Yellow_TLC_TripRecord_NAD83_1M_No_Trip_Info_A.csv"
  private val inputile_B = "/media/cusp/Data/GeoMatch_Files/InputFiles/Yellow_TLC_TripRecord_NAD83_1M_No_Trip_Info_B.csv"
  private val arrPoints_A = Array("Taxi_A_596079", "Taxi_A_645241", "Taxi_A_649367", "Taxi_A_690174", "Taxi_A_720050", "Taxi_A_735836", "Taxi_A_819685", "Taxi_A_828441", "Taxi_A_853934", "Taxi_A_886869", "Taxi_A_27652", "Taxi_A_35532", "Taxi_A_51433", "Taxi_A_60219", "Taxi_A_64504", "Taxi_A_66149", "Taxi_A_67804", "Taxi_A_74422", "Taxi_A_77305", "Taxi_A_87860")
  private val arrPoints_B = Array("Taxi_B_651809")

  def extrat(inputile: String, arrPoints: Array[String]) =
    Source.fromFile(inputile).getLines().foreach(line =>
      if (arrPoints.contains(line.split(",").head))
        println(line)
    )

  def main(args: Array[String]): Unit = {

    extrat(inputile_B, arrPoints_B)
    extrat(inputile_A, arrPoints_A)
  }


}
