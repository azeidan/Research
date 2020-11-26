package org.cusp.bdi.ds.test

import org.cusp.bdi.ds.bt.AVLTree

object TestAVL {

  def main(args: Array[String]): Unit = {

    val iter = Array(3166, 4040, 1924, 7823, 8314, 5288, 8829, 6801, 4944, 3287, 5403, 4001, 2794, 7992, 1239, 2706, 4542, 2137, 8327, 3890, 4903, 1354, 8213)

    val avl = new AVLTree[Int]()

    iter.foreach(avl.getOrElseInsert)

    avl.printIndented()
  }
}
