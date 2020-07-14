package org.cusp.bdi.sknn.util

import com.insightfullogic.quad_trees.Box
import com.insightfullogic.quad_trees.QuadTree
import com.insightfullogic.quad_trees.QuadTreeDigest

trait QuadTreeInfoBase extends Serializable {

    var uniqueIdentifier = -9999

    def copy(other: QuadTreeInfoBase) {

        //        this.assignedPart = other.assignedPart
        this.uniqueIdentifier = other.uniqueIdentifier
    }

    override def toString() =
        "\t%d".format(uniqueIdentifier)
}

class QuadTreeInfo extends QuadTreeInfoBase {

    var quadTree: QuadTree = null

    def this(boundary: Box) {

        this

        quadTree = QuadTree(boundary)
    }

    override def toString() =
        "%s\t%s".format(super.toString, quadTree.toString)
}