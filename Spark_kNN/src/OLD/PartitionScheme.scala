package org.cusp.bdi.sknn

import scala.collection.mutable.HashMap
import scala.collection.immutable.Map
import scala.collection.mutable

object PartitionScheme {

    def apply(startK: Int, rowsPerPartition: Int, mapCenterInfo: mutable.HashMap[Int, Array[Int]], mapKandCounts: Map[Int, Long], mapSchemePhase1: mutable.HashMap[Int, Int]): mutable.HashMap[Int, Int] = {

        val mapScheme = mutable.HashMap[Int, Int]()

        var currK = startK
        var partCounter = 0
        var partRowCount = 0L
        var arrCurrK_Nearest = mapCenterInfo(currK)

        while (mapScheme.size < mapCenterInfo.size) {

            mapScheme.put(currK, partCounter)
            partRowCount = mapKandCounts(currK)
            var i = 0
            while (partRowCount < rowsPerPartition && i < arrCurrK_Nearest.length) {

                if (!mapScheme.contains(arrCurrK_Nearest(i))) {

                    val count = mapKandCounts(arrCurrK_Nearest(i))
                    mapScheme.put(arrCurrK_Nearest(i), partCounter)
                    partRowCount += count
                }
                i += 1
            }

            // next K
            //            i = 0
            while (i < arrCurrK_Nearest.length && mapScheme.contains(arrCurrK_Nearest(i))) i += 1
            if (i == arrCurrK_Nearest.length)
                currK = mapCenterInfo.filter(_._2.length > 0).head._2.head
            else
                currK = arrCurrK_Nearest(i)

            arrCurrK_Nearest = mapCenterInfo(currK)

            partCounter += 1
        }

        mapScheme
    }
}