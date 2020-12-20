package org.cusp.bdi.ds.test

import org.cusp.bdi.ds.sortset.SortedLinkedList

import scala.util.Random

object TestSortedLInkedList {

  def main(args: Array[String]): Unit = {

    //    >>3.88602864,Taxi_1_A_241201,5.86473355,Taxi_1_A_4517,6.73983417,Taxi_1_A_197137,8.67682035,Taxi_1_A_439193,8.78339240,Taxi_1_A_389071,9.99055826,Taxi_1_A_800649,10.04659039,Taxi_1_A_246844,10.53644643,Taxi_1_A_24997,11.98341684,Taxi_1_A_572216,13.17806046,Taxi_1_A_87081,13.89187454,Taxi_1_A_21808
    //    >>6.10610545,Taxi_1_A_178912,13.24470615,Taxi_1_A_999155,16.58020350,Taxi_1_A_609738,19.44289636,Taxi_1_A_175912,20.29770772,Taxi_1_A_22290,20.76372537,Taxi_1_A_159830,21.80549959,Taxi_1_A_257954,23.33148892,Taxi_1_A_966167,24.30580483,Taxi_1_A_838412,25.97788514,Taxi_1_A_100936,28.44622889,Taxi_1_A_991019

    val sortedLInkedList = SortedLinkedList[String](10)

    //    sortedLInkedList.add(3.88602864, "Taxi_1_A_241201")
    //    sortedLInkedList.add(6.73983417, "Taxi_1_A_197137")
    //    sortedLInkedList.add(5.86473355, "Taxi_1_A_4517")
    //    sortedLInkedList.add(8.78339240, "Taxi_1_A_389071")
    //    sortedLInkedList.add(8.67682035, "Taxi_1_A_439193")
    //    sortedLInkedList.add(10.04659039, "Taxi_1_A_246844")
    //    sortedLInkedList.add(10.53644643, "Taxi_1_A_24997")
    //    sortedLInkedList.add(11.98341684, "Taxi_1_A_572216")
    //    sortedLInkedList.add(9.99055826, "Taxi_1_A_800649")
    //    sortedLInkedList.add(13.89187454, "Taxi_1_A_21808")
    //    sortedLInkedList.add(13.17806046, "Taxi_1_A_87081")

    sortedLInkedList.add(0.00000000, "bus_3_a_120356")
    sortedLInkedList.add(0.00000000, "bus_3_a_979829")
    sortedLInkedList.add(3.31886894, "bus_3_a_76465")
    sortedLInkedList.add(1.10628962, "bus_3_a_347976")
    sortedLInkedList.add(1.10628962, "bus_3_a_47413")
    sortedLInkedList.add(1.55772330, "bus_3_a_893993")
    sortedLInkedList.add(1.32771555, "bus_3_a_520644")
    sortedLInkedList.add(2.46781655, "bus_3_a_759745")
    sortedLInkedList.add(2.42825397, "bus_3_a_278855")
    sortedLInkedList.add(2.66289873, "bus_3_a_732026")

//    (0 until 15).foreach(i => sortedLInkedList.add(Random.nextInt(3), i.toString))

    sortedLInkedList.foreach(println)

    println("Length: " + sortedLInkedList.length)
  }
}
