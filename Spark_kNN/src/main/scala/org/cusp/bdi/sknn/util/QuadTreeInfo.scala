package org.cusp.bdi.sknn.util

import com.insightfullogic.quad_trees.{Box, QuadTree}

class QuadTreeInfo {

  var uniqueIdentifier: Int = -9999

  var quadTree: QuadTree = _

  def this(boundary: Box) {

    this

    quadTree = QuadTree(boundary)
  }
  override def toString: String =
    "\t%d\t%s".format(uniqueIdentifier, quadTree.toString())
}