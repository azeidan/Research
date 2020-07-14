//package org.cusp.bdi.sknn
//
//import org.apache.spark.Partitioner
//import org.cusp.bdi.gm.geom.GMGeomBase
//
//case class PartitionerHilbertCluster(hilbertSize: Int, _numPartitions: Int) extends Partitioner {
//
//    override def numPartitions = _numPartitions
//    private def maxIdxCount = (hilbertSize / numPartitions) + 1
//
//    override def getPartition(key: Any): Int =
//        key match { case k: Int => k / maxIdxCount }
//}