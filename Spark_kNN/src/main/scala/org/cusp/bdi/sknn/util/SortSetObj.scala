package org.cusp.bdi.sknn.util

import scala.collection.AbstractIterator
import scala.collection.immutable.Iterable

//object SSTest {
//
//    def main(args: Array[String]): Unit = {
//        val ss = SortSetObj[MatchedPointInfo](10)
//
//        ss.add(1, MatchedPointInfo("A", (0, 0), 1))
//        ss.add(5, MatchedPointInfo("B", (0, 0), 1))
//        ss.add(4, MatchedPointInfo("C", (0, 0), 1))
//        ss.add(1, MatchedPointInfo("D", (0, 0), 1))
//        ss.add(6, MatchedPointInfo("E", (0, 0), 1))
//        ss.add(7, MatchedPointInfo("F", (0, 0), 1))
//        ss.add(8, MatchedPointInfo("G", (0, 0), 1))
//        ss.add(-2, MatchedPointInfo("H", (0, 0), 1))
//        ss.add(3, MatchedPointInfo("I", (0, 0), 1))
//        ss.add(-4, MatchedPointInfo("J", (0, 0), 1))
//        ss.add(1000, MatchedPointInfo("K", (0, 0), 1))
//        ss.add(5, MatchedPointInfo("L", (0, 0), 1))
//        ss.add(1000, MatchedPointInfo("M", (0, 0), 1))
//        ss.add(1, MatchedPointInfo("N", (0, 0), 1))
//        ss.add(-2, MatchedPointInfo("O", (0, 0), 1))
//        ss.add(3, MatchedPointInfo("P", (0, 0), 1))
//        ss.add(-4, MatchedPointInfo("Q", (0, 0), 1))
//        ss.add(5, MatchedPointInfo("R", (0, 0), 1))
//        ss.add(1000, MatchedPointInfo("S", (0, 0), 1))
//        ss.add(1, MatchedPointInfo("T", (0, 0), 1))
//        ss.add(-2, MatchedPointInfo("U", (0, 0), 1))
//        ss.add(3, MatchedPointInfo("V", (0, 0), 1))
//        ss.add(-4, MatchedPointInfo("W", (0, 0), 1))
//        ss.add(-5, MatchedPointInfo("X", (0, 0), 1))
//        ss.add(0, MatchedPointInfo("Y", (0, 0), 1))
//        ss.add(-7, MatchedPointInfo("Z", (0, 0), 1))
//        ss.add(6, MatchedPointInfo("AA", (0, 0), 1))
//        ss.add(-6, MatchedPointInfo("Z", (0, 0), 1))
//        ss.add(606, MatchedPointInfo("AC", (0, 0), 1))
//
//        println(ss)
//    }
//}

//trait NodeData extends Comparable[NodeData] {
//
//    override def toString(): String
//}

//line: String, xy: (Double, Double)

//type T <: Comparable[T]

case class Node(distance: Double, data: Comparable[_]) {

    var next: Node = null

    override def toString() =
        "%f, %s".format(distance, data)
}

case class SortSetObj(maxSize: Int, allowDuplicates: Boolean) extends Serializable with Iterable[Node] {

    private var headNode: Node = null
    private var lastNode: Node = null
    private var nodeCount = 0

    override def head() = headNode
    override def last() = lastNode
    override def size() = nodeCount
    override def isEmpty() = headNode == null

    def clear() = {

        headNode = null
        lastNode = null
        nodeCount = 0
    }

    def isFull() = nodeCount == maxSize
    // line: String, xy: (Double, Double)
    def add(distance: Double, data: Comparable[_]) {

        if (!isFull || distance < last.distance) {

            var prevNode: Node = null
            var currNode = headNode

            while (currNode != null && distance >= currNode.distance) {

                //                if (currNode.line != null && currNode.line.equals(line))
                if (!allowDuplicates && currNode.data.equals(data))
                    return

                prevNode = currNode
                currNode = currNode.next
            }

            nodeCount += 1

            if (prevNode == null) {

                headNode = Node(distance, data)
                headNode.next = currNode

                if (lastNode == null)
                    lastNode = headNode
            }
            else {

                // line, xy
                prevNode.next = Node(distance, data)
                prevNode.next.next = currNode

                if (lastNode == prevNode)
                    lastNode = prevNode.next
            }

            if (nodeCount > maxSize) {

                lastNode = headNode

                (0 until maxSize - 1).foreach(_ => lastNode = lastNode.next)

                lastNode.next = null
                nodeCount -= 1
            }
        }
    }

    def discardAfter(discardFromIndx: Int) = {

        if (!isEmpty && discardFromIndx < size)
            if (discardFromIndx == 0) {

                headNode = null
                lastNode = headNode
                nodeCount = 0
            }
            else {

                lastNode = head

                var idx = 0

                while (lastNode.next != null && idx < discardFromIndx - 1) {

                    lastNode = lastNode.next

                    idx += 1
                }

                //                if (currNode != null) {

                lastNode.next = null
                nodeCount = idx + 1
                //                }
            }
    }

    override def iterator(): Iterator[Node] = new AbstractIterator[Node] {

        var cursor: Node = if (SortSetObj.this.isEmpty) null else headNode

        override def hasNext: Boolean = cursor != null

        override def next(): Node =
            if (!hasNext)
                throw new NoSuchElementException("next on empty Iterator")
            else {
                val ans = cursor
                cursor = cursor.next
                ans
            }
    }

    override def mkString(sep: String) =
        this.map(node => "%s%s".format(node, sep)).mkString("")

    override def toString() =
        mkString("\n")
}