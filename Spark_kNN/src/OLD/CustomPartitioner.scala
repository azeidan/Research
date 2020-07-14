//package org.cusp.bdi.sknn
//
//import org.apache.spark.Partitioner
//
///**
//  * Returns the partition's number for the specified index
//  */
//case class CustomPartitioner(hilbertSize: Int, _numPartitions: Int) extends Partitioner {
//
//    override def numPartitions = _numPartitions
//    private def maxIdxCount = (hilbertSize / numPartitions) + 1
//
//    override def getPartition(key: Any): Int = {
//
//        key.asInstanceOf[(Int, Byte)]._1 / maxIdxCount
//
//        //        val (hIdx, num) = key.asInstanceOf[(Int, Int)]
//        //
//        //        if (keyIdx == 0)
//        //            hIdx % numPartitions
//        //        else
//        //            num / numPartitions // / countPerPartition
//        //        key match {
//        //            case hIdx: Int => {
//        //                //                if (hIdx / maxIdxCount == 245)
//        //                //                    printf("==>%d, %d, %d<==", hIdx, numPartitions, maxIdxCount)
//        //
//        //                hIdx % numPartitions
//        //                //                hIdx / idxCount
//        //            }
//        //        }
//    }
//}