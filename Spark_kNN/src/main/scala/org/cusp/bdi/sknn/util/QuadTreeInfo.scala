package org.cusp.bdi.sknn.util

import com.insightfullogic.quad_trees.{Box, QuadTree}

class QuadTreeInfo {

  var uniqueIdentifier = -9999

  var quadTree: QuadTree = null

  def this(boundary: Box) {

    this

    quadTree = QuadTree(boundary)
  }
  override def toString() =
    "\t%d\t%s".format(uniqueIdentifier, quadTree.toString)
}