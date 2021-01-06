package org.cusp.bdi.ds.test

import org.cusp.bdi.ds.sortset.SortedLinkedList

object TestSortedLInkedList {

  def main(args: Array[String]): Unit = {

    val sortedLInkedList = new SortedLinkedList[String](10)

    sortedLInkedList.add(0.00000000, "bus_3_a_120356")
    sortedLInkedList.add(0.00000000, "bus_3_a_979829")
    sortedLInkedList.add(3.31886894, "bus_3_a_76465")
    sortedLInkedList.add(1.10628962, "bus_3_a_347976")
    sortedLInkedList.add(3.31886894, "bus_3_a_76465_2")
    sortedLInkedList.add(3.31886894, "bus_3_a_76465_3")
    sortedLInkedList.add(3.31886894, "bus_3_a_76465_4")
    sortedLInkedList.add(3.31886894, "bus_3_a_76465_5")
    sortedLInkedList.add(1.10628962, "bus_3_a_47413")
    sortedLInkedList.add(1.55772330, "bus_3_a_893993")
    sortedLInkedList.add(1.32771555, "bus_3_a_520644")
    sortedLInkedList.add(2.46781655, "bus_3_a_759745")
    sortedLInkedList.add(2.42825397, "bus_3_a_278855")
    sortedLInkedList.add(2.66289873, "bus_3_a_732026")

    sortedLInkedList.foreach(println)

    println("Length: " + sortedLInkedList.length)
  }
}
