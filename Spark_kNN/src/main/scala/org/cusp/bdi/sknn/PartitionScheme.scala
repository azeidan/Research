package org.cusp.bdi.sknn

import scala.collection.mutable.HashMap
import scala.collection.immutable.Map

object PartitionScheme {

    def apply(startK: Int, rowsPerPartition: Int, mapCenterInfo: HashMap[Int, Array[Int]], mapKandCounts: Map[Int, Long], mapSchemePhase1: HashMap[Int, Int]) = {

        val mapScheme = HashMap[Int, Int]()

        var currK = startK
        var partCounter = 0
        var partRowCount = 0L
        var arrCurrK_Nearest = mapCenterInfo.get(currK).get

        while (mapScheme.size < mapCenterInfo.size) {

            mapScheme.put(currK, partCounter)
            partRowCount = mapKandCounts.get(currK).get
            var i = 0
            while (partRowCount < rowsPerPartition && i < arrCurrK_Nearest.size) {

                if (mapScheme.get(arrCurrK_Nearest(i)) == None) {

                    val count = mapKandCounts.get(arrCurrK_Nearest(i)).get
                    mapScheme.put(arrCurrK_Nearest(i), partCounter)
                    partRowCount += count
                }
                i += 1
            }

            // next K
            //            i = 0
            while (i < arrCurrK_Nearest.size && mapScheme.get(arrCurrK_Nearest(i)) != None) i += 1
            if (i == arrCurrK_Nearest.size)
                currK = mapCenterInfo.filter(_._2.size > 0).head._2.head
            else
                currK = arrCurrK_Nearest(i)

            arrCurrK_Nearest = mapCenterInfo.get(currK).get

            partCounter += 1
        }

        mapScheme
    }
}