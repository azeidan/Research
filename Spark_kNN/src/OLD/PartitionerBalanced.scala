//package org.cusp.bdi.sknn
//
//import org.apache.spark.Partitioner
//import org.cusp.bdi.gm.geom.GMGeomBase
//
//case class PartitionerBalanced(countPerPartition: Int, _numPartitions: Int) extends Partitioner {
//
//    override def numPartitions = _numPartitions
//
//    override def getPartition(key: Any): Int =
//        key match { case k: Long => k.toInt / countPerPartition % numPartitions }
//}