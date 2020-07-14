//package org.cusp.bdi.sknn
//
//import org.apache.spark.Partitioner
//import org.cusp.bdi.gm.geom.GMGeomBase
//
//case class PartitionerShift(_numPartitions: Int) extends Partitioner {
//
//    override def numPartitions = _numPartitions
//
//    override def getPartition(key: Any): Int = {
//
//        val (datasetMarker, pIdx) = key.asInstanceOf[(Byte, Int)]
//
//        if (datasetMarker == 0)
//            pIdx
//        else
//            (pIdx + 1) % numPartitions
//    }
//}