package org.cusp.bdi.ds.util

import org.cusp.bdi.ds.inter.SpatialIndex

case class SpIndexInfo(dataStruct: SpatialIndex) {

  var uniqueIdentifier: Int = -9999

  override def toString: String =
    "\t%d\t%s".format(uniqueIdentifier, dataStruct.toString)
}