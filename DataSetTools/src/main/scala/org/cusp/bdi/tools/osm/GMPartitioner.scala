package org.cusp.bdi.tools.osm

import org.apache.spark.Partitioner

case class GMPartitioner(_numPartitions: Int) extends Partitioner {

    override def numPartitions = _numPartitions

    override def getPartition(key: Any): Int =
        key match {
            case x: String => Math.abs(x.hashCode()) % numPartitions
            //                if (x == null)
            //                    0
            //                else
            //                    Math.abs(x.hashCode()) % numPartitions
        }
}