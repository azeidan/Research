package org.cusp.bdi.ds.kdt

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.cusp.bdi.ds.SpatialIndex
import org.cusp.bdi.ds.SpatialIndex.{KnnLookupInfo, testAndAddPoint}
import org.cusp.bdi.ds.geom.{Geom2D, Point, Rectangle}
import org.cusp.bdi.ds.kdt.KdTree.{TypeArrLst, TypeMatrixLst, findSearchRegionLocation, nodeCapacity}
import org.cusp.bdi.ds.util.SortedList

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object KdTree extends Serializable {

  type TypeArrLst = Array[ListBuffer[Point]]
  type TypeMatrixLst = Array[TypeArrLst]

  //  type TypeLstOfKeyLst = ListBuffer[(Int, ListBuffer[Point])]
  //  type TypeLstOfKeyKeyLst = ListBuffer[(Int, TypeLstOfKeyLst)]

  val nodeCapacity = 4

  def findSearchRegionLocation(searchRegion: Rectangle, nodeSplitVal: Double, splitX: Boolean): Char = {

    val limits = if (splitX)
      (searchRegion.left, searchRegion.right)
    else
      (searchRegion.bottom, searchRegion.top)

    nodeSplitVal match {
      case sk if sk < limits._1 => 'R' // region to the right (or below) of the splitKey
      case sk if sk > limits._2 => 'L' // region to the left (or above) of the splitKey
      case _ => 'B' // region contains the splitKey
    }
  }
}

abstract class KdtNode extends KryoSerializable {

  def totalPoints: Int

  var rectNodeBounds: Rectangle = _

  override def toString: String =
    "%s\t%,d".format(rectNodeBounds, totalPoints)

  override def write(kryo: Kryo, output: Output): Unit =
    kryo.writeClassAndObject(output, rectNodeBounds)

  override def read(kryo: Kryo, input: Input): Unit =
    rectNodeBounds = kryo.readClassAndObject(input) match {
      case rectangle: Rectangle => rectangle
    }
}

final class KdtBranchRootNode extends KdtNode {

  private var _totalPoints: Int = 0
  var splitVal: Double = 0
  var left: KdtNode = _
  var right: KdtNode = _

  override def totalPoints: Int = _totalPoints

  def totalPoints(totalPoints: Int) {
    this._totalPoints = totalPoints
  }

  def this(splitVal: Double, totalPoints: Int) = {

    this()
    this.splitVal = splitVal
    this._totalPoints = totalPoints
  }

  override def toString: String =
    "%s\t[%,.4f]\t%s %s".format(super.toString, splitVal, if (left == null) '-' else '/', if (right == null) '-' else '\\')

  override def write(kryo: Kryo, output: Output) {

    super.write(kryo, output)
    output.writeInt(_totalPoints)
    output.writeDouble(splitVal)
  }

  override def read(kryo: Kryo, input: Input) {

    super.read(kryo, input)
    _totalPoints = input.readInt()
    splitVal = input.readDouble()
  }
}

final class KdtLeafNode extends KdtNode {

  var lstPoints: ListBuffer[Point] = _

  override def totalPoints: Int = lstPoints.size

  def this(lstPoints: ListBuffer[Point], rectNodeBounds: Rectangle) = {

    this()
    this.lstPoints = lstPoints
    this.rectNodeBounds = rectNodeBounds
  }

  override def write(kryo: Kryo, output: Output): Unit = {

    super.write(kryo, output)

    kryo.writeClassAndObject(output, lstPoints)
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    super.read(kryo, input)

    lstPoints = kryo.readClassAndObject(input).asInstanceOf[ListBuffer[Point]]
  }
}

class KdTree(rectBounds: Rectangle, hgGroupWidth: Int) extends SpatialIndex {

  var root: KdtNode = _

  private val lowerBounds = (rectBounds.left, rectBounds.bottom)

  def getTotalPoints: Int = root.totalPoints

  override def insert(iterPoints: Iterator[Point]): Boolean = {

    if (root != null) throw new IllegalStateException("KD Tree already built")

    if (iterPoints.isEmpty) throw new IllegalStateException("Empty point iterator")

    if (hgGroupWidth < 1) throw new IllegalStateException("Bar width must be >= 1")

    var pointCount = 0

    var matrixHistogram: TypeMatrixLst = null
    val (histogramMaxX, histogramMaxY) = (((rectBounds.right - lowerBounds._1) / hgGroupWidth).toInt, ((rectBounds.top - lowerBounds._2) / hgGroupWidth).toInt)

    val setIdxX = mutable.SortedSet[Int]()
    val setIdxY = mutable.SortedSet[Int]()

    matrixHistogram = new Array(histogramMaxX + 1)

    iterPoints.foreach(pt => {

      pointCount += 1

      val (idxX, idxY) = (((pt.x - lowerBounds._1) / hgGroupWidth).toInt, ((pt.y - lowerBounds._2) / hgGroupWidth).toInt)

      if (matrixHistogram(idxX) == null) {

        matrixHistogram(idxX) = new TypeArrLst(histogramMaxY + 1)
        setIdxX += idxX
      }

      if (matrixHistogram(idxX)(idxY) == null) {

        matrixHistogram(idxX)(idxY) = ListBuffer()
        setIdxY += idxY
      }

      matrixHistogram(idxX)(idxY) += pt
    })

    val arrIdxX = setIdxX.toArray
    val arrIdxY = setIdxY.toArray
    val matrixSplitInfo = MatrixSplitInfo(matrixHistogram, pointCount, arrIdxX, 0, arrIdxX.length - 1, arrIdxY, 0, arrIdxY.length - 1)

    val lstNodeIndo = ListBuffer[(KdtBranchRootNode, Boolean, MatrixSplitInfo, MatrixSplitInfo)]()

    root = buildNode(matrixSplitInfo, splitX = true, lstNodeIndo)

    lstNodeIndo.foreach(row => {

      val (currNode, splitX, msiLeftNode, msiRightNode) = row
      //      if (msiLeftNode.pointCount == 786)
      //        println
      currNode.left = buildNode(msiLeftNode, !splitX, lstNodeIndo)
      currNode.right = buildNode(msiRightNode, !splitX, lstNodeIndo)
    })

    this.root match {
      case kdtBRN: KdtBranchRootNode =>
        updateBoundsAndTotalPoint(kdtBRN)
      case _ =>
    }

    true
  }

  private def buildNode(matrixSplitInfo: MatrixSplitInfo, splitX: Boolean, lstNodeInfo: ListBuffer[(KdtBranchRootNode, Boolean, MatrixSplitInfo, MatrixSplitInfo)]) =
    matrixSplitInfo match {

      case null =>
        null
      case _ =>

        if (matrixSplitInfo.pointCount <= nodeCapacity || !matrixSplitInfo.canPartition(splitX)) {

          val pointInf = matrixSplitInfo.extractPointInfo()
          new KdtLeafNode(pointInf._1, pointInf._2)
        }
        else {

          val matrixSplitInfoParts = matrixSplitInfo.partition(splitX)

          val splitVal = matrixSplitInfoParts._1.getSplitIdx(splitX) * hgGroupWidth + hgGroupWidth + (if (splitX) lowerBounds._1 else lowerBounds._2) - 1e-6

          val kdtBranchRootNode = new KdtBranchRootNode(splitVal, matrixSplitInfo.pointCount)

          lstNodeInfo += ((kdtBranchRootNode, splitX, matrixSplitInfoParts._1, matrixSplitInfoParts._2))

          kdtBranchRootNode
        }
    }

  override def findExact(searchXY: (Double, Double)): Point = {

    var currNode = this.root
    var splitX = true

    while (currNode != null)
      currNode match {
        case kdtBranchRootNode: KdtBranchRootNode =>

          currNode = if ((if (splitX) searchXY._1 else searchXY._2) <= kdtBranchRootNode.splitVal)
            kdtBranchRootNode.left
          else
            kdtBranchRootNode.right

          splitX = !splitX

        case kdtLeafNode: KdtLeafNode =>
          return kdtLeafNode.lstPoints.find(pt => pt.x.equals(searchXY._1) && pt.y.equals(searchXY._2)).orNull
      }

    null
  }

  def findBestNode(searchPoint: Geom2D, k: Int): (KdtNode, Boolean) = {

    // find leaf containing point
    var currNode = root
    var splitX = true

    while (true)
      currNode match {
        case kdtBRN: KdtBranchRootNode =>

          if ((if (splitX) searchPoint.x else searchPoint.y) <= kdtBRN.splitVal)
            if (kdtBRN.left != null && kdtBRN.left.totalPoints >= k)
              currNode = kdtBRN.left
            else
              return (currNode, splitX)
          else //
            if (kdtBRN.right != null && kdtBRN.right.totalPoints >= k)
              currNode = kdtBRN.right
            else
              return (currNode, splitX)

          splitX = !splitX

        case _: KdtNode =>
          return (currNode, splitX)
      }

    null
  }

  override def nearestNeighbor(searchPoint: Point, sortSetSqDist: SortedList[Point]) {

    //        if (searchPoint.userData.toString.equalsIgnoreCase("yellow_1_a_419114"))
    //          println

    var (sPtBestNode, splitX) = findBestNode(searchPoint, sortSetSqDist.maxSize)

    val knnLookupInfo = new KnnLookupInfo(searchPoint, sortSetSqDist, sPtBestNode.rectNodeBounds)

    def process(kdtNode: KdtNode, skipKdtNode: KdtNode) {

      val lstKdtNode = ListBuffer((kdtNode, splitX))

      lstKdtNode.foreach(row =>
        if (row._1 != skipKdtNode)
          row._1 match {
            case kdtBRN: KdtBranchRootNode =>

              findSearchRegionLocation(knnLookupInfo.rectSearchRegion, kdtBRN.splitVal, row._2) match {
                case 'L' =>
                  if (kdtBRN.left != null && knnLookupInfo.rectSearchRegion.intersects(kdtBRN.left.rectNodeBounds))
                    lstKdtNode += ((kdtBRN.left, !row._2))
                case 'R' =>
                  if (kdtBRN.right != null && knnLookupInfo.rectSearchRegion.intersects(kdtBRN.right.rectNodeBounds))
                    lstKdtNode += ((kdtBRN.right, !row._2))
                case _ =>
                  if (kdtBRN.left != null && knnLookupInfo.rectSearchRegion.intersects(kdtBRN.left.rectNodeBounds))
                    lstKdtNode += ((kdtBRN.left, !row._2))
                  if (kdtBRN.right != null && knnLookupInfo.rectSearchRegion.intersects(kdtBRN.right.rectNodeBounds))
                    lstKdtNode += ((kdtBRN.right, !row._2))
              }

            case kdtLeafNode: KdtLeafNode =>
              if (knnLookupInfo.rectSearchRegion.intersects(kdtLeafNode.rectNodeBounds))
                kdtLeafNode.lstPoints.foreach(testAndAddPoint(_, knnLookupInfo))
          }
      )
    }

    process(sPtBestNode, null)

    if (sPtBestNode != this.root) {

      splitX = true
      process(this.root, sPtBestNode)
    }
  }

  //  private def partitionMatrix(matrixSplit: MatrixSplitInfo, pointCountInMap: Int, splitX: Boolean, lowerBounds: (Double, Double)) = {
  //
  //    var splitVal = -1.0
  //    var countLTEQ = 0
  //
  //    //      }
  //    //
  //    //
  //    //      val iterX = matrixHistogram.iterator
  //    //
  //    //      while (iterX.hasNext) {
  //    //
  //    //        val lstKeyXY = iterX.next
  //    //        var size = lstKeyXY._2.map(_._2.size).sum
  //    //
  //    //        if (countLTEQ + size <= pointCountHalf) {
  //    //
  //    //          lstKeyXY_LEQ += lstKeyXY
  //    //
  //    //          countLTEQ += size
  //    //        } else {
  //    //
  //    //          val lstParts = lstKeyXY._2.map(row => row._2.map(pt => (row._1, pt))).flatMap(_.seq).sortBy(_._2.x).splitAt(pointCountHalf - countLTEQ)
  //    //
  //    //          countLTEQ += lstParts._1.size
  //    //
  //    //          splitVal = lstParts._2.head._2.x - 1e-6
  //    //
  //    //          lstKeyXY_LEQ += ((lstKeyXY._1, lstParts._1.groupBy(_._1).mapValues(_.map(_._2)).to[ListBuffer].sortBy(_._1)))
  //    //          lstKeyXY_GT += ((lstKeyXY._1, lstParts._2.groupBy(_._1).mapValues(_.map(_._2)).to[ListBuffer].sortBy(_._1)))
  //    //
  //    //          lstKeyXY_GT ++= iterX
  //    //        }
  //    //      }
  //    //    }
  //    //    else {
  //    //
  //    //      val iterKeyY_Count = matrixHistogram.map(_._2).flatMap(_.seq).sortBy(_._1).iterator
  //    //      var lstKeyY_Lst: ListBuffer[(Int, ListBuffer[Point])] = null
  //    //      var count = 0
  //    //      var splitKeyY = -1
  //    //      var continue = true
  //    //
  //    //      while (continue) {
  //    //
  //    //        val keyY_Lst = iterKeyY_Count.next
  //    //
  //    //        if (count + keyY_Lst._2.size <= pointCountHalf || splitKeyY == keyY_Lst._1) {
  //    //
  //    //          if (splitKeyY != keyY_Lst._1)
  //    //            lstKeyY_Lst = ListBuffer()
  //    //
  //    //          splitKeyY = keyY_Lst._1
  //    //          lstKeyY_Lst += keyY_Lst
  //    //          count += keyY_Lst._2.size
  //    //        }
  //    //        else {
  //    //
  //    //          splitVal = lstKeyY_Lst.map(_._2.map(_.y)).flatMap(_.seq).sortBy(identity).take(pointCountHalf).last
  //    //
  //    //          continue = false
  //    //        }
  //    //      }
  //    //
  //    //      val iterX = matrixHistogram.iterator
  //    //
  //    //      while (iterX.hasNext) {
  //    //
  //    //        val lstKeyXY = iterX.next
  //    //
  //    //        val lstLEQ = new TypeLstOfKeyLst()
  //    //        val lstGT = new TypeLstOfKeyLst()
  //    //
  //    //        lstKeyXY._2.foreach(lstKeyY => {
  //    //          lstKeyY._1.compareTo(splitKeyY).signum match {
  //    //            case 1 =>
  //    //              lstGT += lstKeyY
  //    //            case -1 =>
  //    //              lstLEQ += lstKeyY
  //    //              countLTEQ = lstKeyY._2.size
  //    //            case 0 =>
  //    //              val lstParts = lstKeyY._2.partition(_.y <= splitKeyY)
  //    //
  //    //              countLTEQ += lstParts._1.size
  //    //
  //    //              if (lstParts._1.nonEmpty)
  //    //                lstLEQ += ((lstKeyY._1, lstParts._1))
  //    //              if (lstParts._2.nonEmpty)
  //    //                lstGT += ((lstKeyY._1, lstParts._2))
  //    //          }
  //    //
  //    //          lstKeyXY_LEQ += ((lstKeyXY._1, lstLEQ))
  //    //          lstKeyXY_GT += ((lstKeyXY._1, lstGT))
  //    //        })
  //    //      }
  //    //    }
  //    //
  //    //    (splitVal, lstKeyXY_LEQ, countLTEQ, lstKeyXY_GT, pointCountInMap - countLTEQ)
  //  }


  //  private def partitionMatrix(lstKeyXY: TypeLstOfKeyKeyLst, pointCountInMap: Int, splitX: Boolean, lowerBounds: (Double, Double)) = {
  //
  //    val lstKeyXY_LEQ = new TypeLstOfKeyKeyLst()
  //    val lstKeyXY_GT = new TypeLstOfKeyKeyLst()
  //
  //    var meanCoord = -1.0
  //    var countLTEQ = 0
  //
  //    if (lstKeyXY.size > 1 || lstKeyXY.head._2.size > 1)
  //      if (splitX) {
  //
  //        //        if (pointCountInMap == 6395) {
  //        //          //          lstKeyXY.foreach(kxLst => {
  //        //          //            val lst = kxLst._2.map(kyLst => "%d\t%s".format(kyLst._1, kyLst._2.map(pt => "%.2f\t%.2f%n".format(pt.x, pt.y))))
  //        //          //
  //        //          //            println("%d\t%s".format(kxLst._1, lst.mkString("\t")))
  //        //          //          })
  //        //
  //        //          val count = lstKeyXY.map(keyXY_Lst => keyXY_Lst._2.map(_._2.size).sum).sum
  //        //          val sum = lstKeyXY.map(keyXY_Lst => keyXY_Lst._1.toDouble * keyXY_Lst._2.map(_._2.size).sum).sum
  //        //
  //        //        }
  //        val meanHG = lstKeyXY.map(keyXY_Lst => keyXY_Lst._1.toDouble * keyXY_Lst._2.map(_._2.size).sum).sum / pointCountInMap
  //        val meanHG_flr = meanHG.toInt
  //        meanCoord = meanHG - 1 + lowerBounds._1
  //
  //        // partition
  //        val lstLEQ_Y = new TypeLstOfKeyLst()
  //        val lstGT_Y = new TypeLstOfKeyLst()
  //        val iter = lstKeyXY.iterator
  //
  //        while (iter.hasNext) {
  //
  //          val keyXY_Lst = iter.next
  //
  //          keyXY_Lst._1.compareTo(meanHG_flr).signum match {
  //            case 1 =>
  //              lstKeyXY_LEQ += keyXY_Lst
  //            case -1 =>
  //              lstKeyXY_LEQ += keyXY_Lst
  //              countLTEQ += keyXY_Lst._2.size
  //            case _ =>
  //
  //              keyXY_Lst._2.foreach(keyY_Lst => {
  //
  //                val lstParts = keyY_Lst._2.partition(_.x <= meanCoord)
  //
  //                if (lstParts._1.nonEmpty) {
  //
  //                  lstLEQ_Y += ((keyY_Lst._1, lstParts._1))
  //                  countLTEQ += lstParts._1.size
  //                }
  //
  //                if (lstParts._2.nonEmpty)
  //                  lstGT_Y += ((keyY_Lst._1, lstParts._2))
  //              })
  //
  //              if (lstLEQ_Y.nonEmpty) lstKeyXY_LEQ += ((keyXY_Lst._1, lstLEQ_Y))
  //              if (lstGT_Y.nonEmpty) lstKeyXY_GT += ((keyXY_Lst._1, lstGT_Y))
  //
  //              // done the rest is just >
  //              iter.foreach(lstKeyXY_GT += _)
  //          }
  //        }
  //      }
  //      else {
  //
  //        val meanHG = lstKeyXY.map(_._2).map(_.map(keyY_Lst => keyY_Lst._1.toDouble * keyY_Lst._2.size).sum).sum / pointCountInMap
  //        val meanHG_flr = meanHG.toInt
  //        meanCoord = meanHG - 1 + lowerBounds._2
  //
  //        lstKeyXY.foreach(keyXY_Lst => {
  //
  //          val lstLEQ_Y = new TypeLstOfKeyLst()
  //          val lstGT_Y = new TypeLstOfKeyLst()
  //          val iter = keyXY_Lst._2.iterator
  //
  //          while (iter.hasNext) {
  //
  //            val keyY_Lst = iter.next
  //
  //            keyY_Lst._1.compareTo(meanHG_flr).signum match {
  //              case -1 =>
  //                lstGT_Y += keyY_Lst
  //              case 1 =>
  //                lstLEQ_Y += keyY_Lst
  //                countLTEQ += keyY_Lst._2.size
  //              case _ =>
  //
  //                val lstParts = keyY_Lst._2.partition(_.y <= meanCoord)
  //
  //                if (lstParts._1.nonEmpty) {
  //
  //                  lstLEQ_Y += ((keyY_Lst._1, lstParts._1))
  //                  countLTEQ += lstParts._1.size
  //                }
  //
  //                if (lstParts._2.nonEmpty)
  //                  lstGT_Y += ((keyY_Lst._1, lstParts._2))
  //
  //                // done the rest is just >
  //                iter.foreach(lstGT_Y += _)
  //            }
  //          }
  //
  //          if (lstLEQ_Y.nonEmpty) lstKeyXY_LEQ += ((keyXY_Lst._1, lstLEQ_Y))
  //          if (lstGT_Y.nonEmpty) lstKeyXY_GT += ((keyXY_Lst._1, lstGT_Y))
  //        })
  //      }
  //
  //    (meanCoord, lstKeyXY_LEQ, countLTEQ, lstKeyXY_GT, pointCountInMap - countLTEQ)
  //
  //    /*
  //    private def partitionMatrix(mapHG_node: TypeMapKeyXY_Lst, pointCountInMap: Int, splitX: Boolean) = {
  //
  //    val mapLEQ = new TypeMapKeyXY_Lst()
  //    val mapGT = new TypeMapKeyXY_Lst()
  //
  //    var meanHG = -1.0
  //    var countLTEQ = 0
  //
  //    if (mapHG_node.size > 1)
  //      if (splitX) {
  //
  //        meanHG = mapHG_node.map(keyXY_Lst => keyXY_Lst._1._1 * keyXY_Lst._2.size).sum / pointCountInMap
  //        val meanHG_flr: Double = meanHG.toLong
  //
  //        mapHG_node.foreach(keyXY_Lst =>
  //          keyXY_Lst._1._1.compareTo(meanHG_flr).signum match {
  //            case -1 =>
  //              mapLEQ += keyXY_Lst
  //              countLTEQ += keyXY_Lst._2.size
  //            case 1 =>
  //              mapGT += keyXY_Lst
  //            case _ =>
  //
  //              val lstParts = keyXY_Lst._2.partition(_.x <= meanHG)
  //
  //              if (lstParts._1.nonEmpty) {
  //
  //                mapLEQ += ((keyXY_Lst._1, lstParts._1))
  //                countLTEQ += lstParts._1.size
  //              }
  //
  //              if (lstParts._2.nonEmpty)
  //                mapGT += ((keyXY_Lst._1, lstParts._2))
  //          }
  //        )
  //      }
  //      else {
  //
  //        meanHG = mapHG_node.map(keyXY_Lst => keyXY_Lst._1._2 * keyXY_Lst._2.size).sum / pointCountInMap
  //        val meanHG_flr: Double = meanHG.toLong
  //
  //        mapHG_node.foreach(keyXY_Lst =>
  //          keyXY_Lst._1._2.compareTo(meanHG_flr).signum match {
  //            case -1 =>
  //              mapLEQ += keyXY_Lst
  //              countLTEQ += keyXY_Lst._2.size
  //            case 1 =>
  //              mapGT += keyXY_Lst
  //            case _ =>
  //
  //              val lstParts = keyXY_Lst._2.partition(_.y <= meanHG)
  //
  //              if (lstParts._1.nonEmpty) {
  //
  //                mapLEQ += ((keyXY_Lst._1, lstParts._1))
  //                countLTEQ += lstParts._1.size
  //              }
  //
  //              if (lstParts._2.nonEmpty)
  //                mapGT += ((keyXY_Lst._1, lstParts._2))
  //          }
  //        )
  //      }private def partitionMatrix(mapHG_node: TypeMapKeyXY_Lst, pointCountInMap: Int, splitX: Boolean) = {
  //
  //    val mapLEQ = new TypeMapKeyXY_Lst()
  //    val mapGT = new TypeMapKeyXY_Lst()
  //
  //    var meanHG = -1.0
  //    var countLTEQ = 0
  //
  //    if (mapHG_node.size > 1)
  //      if (splitX) {
  //
  //        meanHG = mapHG_node.map(keyXY_Lst => keyXY_Lst._1._1 * keyXY_Lst._2.size).sum / pointCountInMap
  //        val meanHG_flr: Double = meanHG.toLong
  //
  //        mapHG_node.foreach(keyXY_Lst =>
  //          keyXY_Lst._1._1.compareTo(meanHG_flr).signum match {
  //            case -1 =>
  //              mapLEQ += keyXY_Lst
  //              countLTEQ += keyXY_Lst._2.size
  //            case 1 =>
  //              mapGT += keyXY_Lst
  //            case _ =>
  //
  //              val lstParts = keyXY_Lst._2.partition(_.x <= meanHG)
  //
  //              if (lstParts._1.nonEmpty) {
  //
  //                mapLEQ += ((keyXY_Lst._1, lstParts._1))
  //                countLTEQ += lstParts._1.size
  //              }
  //
  //              if (lstParts._2.nonEmpty)
  //                mapGT += ((keyXY_Lst._1, lstParts._2))
  //          }
  //        )
  //      }
  //      else {
  //
  //        meanHG = mapHG_node.map(keyXY_Lst => keyXY_Lst._1._2 * keyXY_Lst._2.size).sum / pointCountInMap
  //        val meanHG_flr: Double = meanHG.toLong
  //
  //        mapHG_node.foreach(keyXY_Lst =>
  //          keyXY_Lst._1._2.compareTo(meanHG_flr).signum match {
  //            case -1 =>
  //              mapLEQ += keyXY_Lst
  //              countLTEQ += keyXY_Lst._2.size
  //            case 1 =>
  //              mapGT += keyXY_Lst
  //            case _ =>
  //
  //              val lstParts = keyXY_Lst._2.partition(_.y <= meanHG)
  //
  //              if (lstParts._1.nonEmpty) {
  //
  //                mapLEQ += ((keyXY_Lst._1, lstParts._1))
  //                countLTEQ += lstParts._1.size
  //              }
  //
  //              if (lstParts._2.nonEmpty)
  //                mapGT += ((keyXY_Lst._1, lstParts._2))
  //          }
  //        )
  //      }
  //     */
  //
  //    //    if (splitX)
  //    //      mapHG_node.keys.to[mutable.SortedSet].foreach(keyX => {
  //    //
  //    //        var mapAddTo = (if (splitVal == -1.0) mapLEQ else mapGT)
  //    //          .getOrElseUpdate(keyX, new TypeMapKeyLst())
  //    //
  //    //        mapHG_node(keyX).foreach(keyYLst => {
  //    //
  //    //          mapAddTo += keyYLst
  //    //
  //    //          if (splitVal == -1) {
  //    //
  //    //            max = keyYLst._2.maxBy(_.x).x
  //    //            countLTEQ += keyYLst._2.size
  //    //
  //    //            if (countLTEQ >= pointLimit) {
  //    //
  //    //              splitVal = max
  //    //              mapAddTo = mapGT.getOrElseUpdate(keyX, new TypeMapKeyLst())
  //    //            }
  //    //          }
  //    //        })
  //    //      })
  //    //    else {
  //    //
  //    //      val arrKeyY = iterKeyY.toArray
  //    //
  //    //      if (arrKeyY.length > 1)
  //    //        arrKeyY.sortBy(x => (x._2, x._1))
  //    //          .foreach(row => {
  //    //
  //    //            val map = (if (splitVal == -1.0) mapLEQ else mapGT)
  //    //              .getOrElseUpdate(row._1, new TypeMapKeyLst())
  //    //
  //    //            map += ((row._2, row._3))
  //    //
  //    //            if (splitVal == -1) {
  //    //
  //    //              max = row._3.maxBy(_.y).y
  //    //              countLTEQ += row._3.size
  //    //
  //    //              if (countLTEQ >= pointLimit)
  //    //                splitVal = max
  //    //            }
  //    //          })
  //    //    }
  //    //
  //    //
  //    //    (splitVal, mapLEQ, countLTEQ, mapGT, pointCountInMap - countLTEQ)
  //  }

  //  private def partitionMatrix(mapHG_node: TypeMapKeyMap, splitVal_key: Double, splitX: Boolean) = {
  //
  //    val mapLEQ = new TypeMapKeyMap()
  //    val mapGT = new TypeMapKeyMap()
  //
  //    if (splitX)
  //      mapHG_node.foreach(keyMap => (if (keyMap._1.compare(splitVal_key) <= 0) mapLEQ else mapGT) += keyMap)
  //    else
  //      mapHG_node.foreach(keyMap => {
  //
  //        val mapLEQ_Y = new TypeMapKeyLst()
  //        val mapGT_Y = new TypeMapKeyLst()
  //
  //        keyMap._2.foreach(keyLst => (if (keyLst._1.compare(splitVal_key) <= 0) mapLEQ_Y else mapGT_Y) += keyLst)
  //
  //        if (mapLEQ_Y.nonEmpty) mapLEQ += ((keyMap._1, mapLEQ_Y))
  //
  //        if (mapGT_Y.nonEmpty) mapGT += ((keyMap._1, mapGT_Y))
  //      })
  //
  //    (mapLEQ, mapGT)
  //  }
  //  private def partitionMatrix(mapHG_node: mutable.HashMap[Double, mutable.HashMap[Double, ListBuffer[Point]]], splitVal_key: Double, splitX: Boolean) = {
  //
  //    val mapLT = mutable.HashMap[Double, mutable.HashMap[Double, ListBuffer[Point]]]()
  //    val mapEQ = mutable.HashMap[Double, mutable.HashMap[Double, ListBuffer[Point]]]()
  //    val mapGT = mutable.HashMap[Double, mutable.HashMap[Double, ListBuffer[Point]]]()
  //
  //    if (splitX)
  //      mapHG_node.foreach(keyMap =>
  //        (keyMap._1.compare(splitVal_key).signum match {
  //          case -1 => mapLT
  //          case 0 => mapEQ
  //          case _ => mapGT
  //        })
  //          += keyMap
  //      )
  //    else
  //      mapHG_node.foreach(keyMap => {
  //
  //        val mapLT_Y = mutable.HashMap[Double, ListBuffer[Point]]()
  //        val mapEQ_Y = mutable.HashMap[Double, ListBuffer[Point]]()
  //        val mapGT_Y = mutable.HashMap[Double, ListBuffer[Point]]()
  //
  //        keyMap._2.foreach(keyLst =>
  //          (keyLst._1.compare(splitVal_key).signum match {
  //            case -1 => mapLT_Y
  //            case 0 => mapEQ_Y
  //            case _ => mapGT_Y
  //          })
  //            += keyLst
  //        )
  //
  //        if (mapLT_Y.nonEmpty)
  //          mapLT += ((keyMap._1, mapLT_Y))
  //        if (mapEQ_Y.nonEmpty)
  //          mapEQ += ((keyMap._1, mapEQ_Y))
  //        if (mapGT_Y.nonEmpty)
  //          mapGT += ((keyMap._1, mapGT_Y))
  //      })
  //
  //    (mapLT, mapEQ, mapGT)
  //  }

  override def toString: String =
    "%s".format(root)

  override def write(kryo: Kryo, output: Output): Unit = {

    val lstNode = ListBuffer(this.root)

    lstNode.foreach(kdtNode => {

      kryo.writeClassAndObject(output, kdtNode)

      kdtNode match {
        case brn: KdtBranchRootNode =>
          lstNode += (brn.left, brn.right)
        case _ =>
      }
    })
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    def readNode() = kryo.readClassAndObject(input) match {
      case kdtNode: KdtNode => kdtNode
    }

    this.root = readNode()

    val lstNode = ListBuffer(this.root)

    lstNode.foreach {
      case brn: KdtBranchRootNode =>
        brn.left = readNode()
        brn.right = readNode()

        lstNode += (brn.left, brn.right)
      case _ =>
    }
  }

  private def updateBoundsAndTotalPoint(kdtBranchRootNode: KdtBranchRootNode) {

    if (kdtBranchRootNode.left != null)
      kdtBranchRootNode.left match {
        case kdtBRN: KdtBranchRootNode =>
          updateBoundsAndTotalPoint(kdtBRN)
        case _ =>
      }

    if (kdtBranchRootNode.right != null)
      kdtBranchRootNode.right match {
        case kdtBRN: KdtBranchRootNode =>
          updateBoundsAndTotalPoint(kdtBRN)
        case _ =>
      }

    if (kdtBranchRootNode.left != null) {

      kdtBranchRootNode.rectNodeBounds = new Rectangle(kdtBranchRootNode.left.rectNodeBounds)
      //      kdtBranchRootNode.totalPoints(kdtBranchRootNode.left.totalPoints)
    }

    if (kdtBranchRootNode.right != null) {

      if (kdtBranchRootNode.rectNodeBounds == null)
        kdtBranchRootNode.rectNodeBounds = new Rectangle(kdtBranchRootNode.right.rectNodeBounds)
      else
        kdtBranchRootNode.rectNodeBounds.mergeWith(kdtBranchRootNode.right.rectNodeBounds)

      //      kdtBranchRootNode.totalPoints(kdtBranchRootNode.totalPoints + kdtBranchRootNode.right.totalPoints)
    }
  }
}