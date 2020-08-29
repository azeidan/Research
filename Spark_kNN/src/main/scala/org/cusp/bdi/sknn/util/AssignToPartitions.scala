//package org.cusp.bdi.sknn.util
//
//import scala.collection.mutable.ListBuffer
//
//import org.cusp.bdi.sknn.PartitionInfo
//
///**
// * Based on Maximum Subarray algorithm (Kadane's)
// */
//case class AssignToPartitions(arrPartInf: Array[PartitionInfo], maxSum: Long) {
//
//  private var partCounter = 0
//
//  // group of quadtrees adding to the maximum sum
//  private val lstGroup = ListBuffer[PartitionInfo]()
//
//  def getPartitionCount: Int = partCounter
//
//  // 1st, assign quadtrees with maximum total points
//  arrPartInf.foreach(qtInf =>
//    if (qtInf.totalPoints >= maxSum) {
//
//      qtInf.assignedPart = partCounter
//      partCounter += 1
//    })
//
//  //  unassigned regions
//  var arrUnassigned: Array[PartitionInfo] = arrPartInf.filter(_.assignedPart == -1).sortBy(_.totalPoints)
//
//  while (!arrUnassigned.isEmpty) {
//
//    var maxSoFar = 0L
//    var maxSumEndingHere = 0L
//    var idx = 0
//
//    while (idx < arrUnassigned.length) {
//
//      if (maxSumEndingHere + arrUnassigned(idx).totalPoints <= maxSum && lstGroup.size != arrUnassigned.length) {
//
//        maxSumEndingHere += arrUnassigned(idx).totalPoints
//
//        lstGroup += arrUnassigned(idx)
//
//        // TODO: remove, for negative values
//        if (maxSumEndingHere < 0) {
//
//          lstGroup.clear()
//          maxSumEndingHere = 0
//        }
//        else if (maxSoFar < maxSumEndingHere)
//          maxSoFar = maxSumEndingHere
//      }
//      else {
//
//        lstGroup.foreach(_.assignedPart = partCounter)
//
//        lstGroup.clear()
//
//        partCounter += 1
//
//        idx = arrUnassigned.length
//      }
//
//      idx += 1
//    }
//
//    arrUnassigned = arrPartInf.filter(_.assignedPart == -1).sortBy(_.totalPoints)
//  }
//}