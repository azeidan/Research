package org.cusp.bdi.sknn.util

import scala.collection.mutable.ListBuffer

import org.cusp.bdi.sknn.QTPartId

/**
  * Based on Maximum Subarray algorithm (Kadane's)
  */
case class AssignToPartitions(arrAttrQT: Array[QTPartId], maxSum: Long) {

    private var partCounter = 0

    // group of quadtrees adding to the maximum sum
    private val lstGroup = ListBuffer[QTPartId]()

    def getPartitionCount() = partCounter

    // 1st, assign quadtrees with maximum total points
    arrAttrQT.foreach(qtInf =>
        if (qtInf.totalPoints >= maxSum) {

            qtInf.assignedPart = partCounter
            partCounter += 1
        })

    // remaining unassigned quadtrees
    var arrUnassigned = arrAttrQT.filter(_.assignedPart == -1).sortBy(_.totalPoints)

    while (!arrUnassigned.isEmpty) {

        var maxSoFar = 0L
        var maxSumEndingHere = 0L
        var idx = 0

        while (idx < arrUnassigned.size) {

            if (maxSumEndingHere + arrUnassigned(idx).totalPoints <= maxSum && lstGroup.size != arrUnassigned.size) {

                maxSumEndingHere += arrUnassigned(idx).totalPoints

                lstGroup += arrUnassigned(idx)

                // TODO: remove, for negative values
                if (maxSumEndingHere < 0) {

                    lstGroup.clear()
                    maxSumEndingHere = 0
                }
                else if (maxSoFar < maxSumEndingHere)
                    maxSoFar = maxSumEndingHere
            }
            else {

                lstGroup.foreach(_.assignedPart = partCounter)

                lstGroup.clear()

                partCounter += 1

                idx = arrUnassigned.size
            }

            idx += 1
        }

        arrUnassigned = arrAttrQT.filter(_.assignedPart == -1).sortBy(_.totalPoints)
    }
}