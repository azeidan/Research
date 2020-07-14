//package org.cusp.bdi.sknn
//
//import org.apache.spark.Partitioner
//
///**
//  * Returns the partition's number for the specified index
//  */
//case class CustomPartitioner2(countPerPartition: Int, _numPartitions: Int) extends Partitioner {
//
//    override def numPartitions = _numPartitions
//
//    //    private def maxIdxCount = Math.ceil(hilbertSize - 1 / numPartitions.toDouble).toInt
//
//    override def getPartition(key: Any): Int = {
//
//        val (datasetMarker, rowId) = key.asInstanceOf[(Byte, Long)]
//
//        (rowId / countPerPartition).toInt % numPartitions
//
//        //        key match {
//        //            case k: Int => {
//        //                //                if (hIdx / maxIdxCount == 245)
//        //                //                    printf("==>%d, %d, %d<==", hIdx, numPartitions, maxIdxCount)
//        //
//        //                k * numPartitions / countPerPartition
//        //            }
//        //        }
//    }
//}