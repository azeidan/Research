/**
  * Copyright (c) 2019, The City University of New York and the University of Helsinki
  * All rights reserved.
  */

package org.cusp.bdi.sknn.util

/**
  * Wrapper class for Long pass-by-reference
  */
private case class RefLong(var i: Long) {}

/**
  * Computes the Hilbert Index for a given coordinates
  *
  * The class assumes that the square is divided into (n X n) cells. Where
  * n is a power of 2. The box's lower left coordinates are (0,0) and the upper right coordinates are (n − 1, n − 1).
  *
  * Code translated from https://en.wikipedia.org/wiki/Hilbert_curve
  */
object HilbertIndex_V2 {

    //    def main(args: Array[String]): Unit = {
    //        println(computeIndex(32768, (32767L, 254L)))
    //        println(computeIndex(32768, (32767L, 255L)))
    //        println(computeIndex(32768, (32767L, 2540L)))
    //
    //        println(reverseIndex(32768, 1073676289))
    //    }

    /**
      * Returns the index of the given Hilbert box coordinates
      *
      * If the indexes of the box are out of range, -1 is returned.
      * Else, a value between 0 and (n-1) will be returned
      *
      * @param n, Hilbert's n
      * @param xy, the X and Y coordinates of the Hilbert box
      */
    def computeIndex(n: Long, xy: (Long, Long)) = {

        // if xy are out of range, don't compute the index
        //        if (xy._1xy._1 < n && xy._2 < n)
        mapToIndex(n, new RefLong(xy._1), new RefLong(xy._2))
        //        else
        //            -1
    }

    def reverseIndex(n: Long, hIdx: Long) = {

        var rx = false
        var ry = false
        var t = hIdx

        val x = new RefLong(0)
        val y = new RefLong(0)

        var s = 1L
        while (s < n) {

            rx = (t & 2) != 0;
            ry = ((t ^ (if (rx) 1 else 0)) & 1) != 0;

            rotate(s, x, y, rx, ry)
            x.i += (if (rx) s else 0)
            y.i += (if (ry) s else 0)
            t >>>= 2 // t/= 4

            s <<= 1 // s *= 2
        }

        (x.i, y.i)
    }

    //  -----------
    // | ↖ | ↑ | ↗ |
    // |---|---|---|
    // | ← | X | → |
    // |---|---|---|
    // | ↙ | ↓ | ↘ |
    //  -----------
    def getNearbyIndexes(n: Long, hIdx: Long) = {

        val boxCoords = reverseIndex(n, hIdx)

        //        val lst = List((boxCoords._1 - 1, boxCoords._2 - 1), (boxCoords._1 - 1, boxCoords._2), (boxCoords._1 - 1, boxCoords._2 + 1), (boxCoords._1, boxCoords._2 + 1),
        //            (boxCoords._1 + 1, boxCoords._2 + 1), (boxCoords._1 + 1, boxCoords._2), (boxCoords._1 + 1, boxCoords._2 - 1), (boxCoords._1, boxCoords._2 - 1))
        //
        //        println("$$$$$$$$$$$$$$$$$$$$$$$$$")
        //        println(lst)
        //        println("$$$$$$$$$$$$$$$$$$$$$$$$$")
        //        println(lst
        //            .filter(coord => {
        //
        //                val a = coord._1 >= 0 && coord._1 < n
        //                val b = coord._2 >= 0 && coord._2 < n
        //
        //                val ret = coord._1 >= 0 && coord._1 < n && coord._2 >= 0 && coord._2 < n
        //
        //                ret
        //
        //            }))
        //        println("$$$$$$$$$$$$$$$$$$$$$$$$$")
        List((boxCoords._1 - 1, boxCoords._2 - 1), (boxCoords._1 - 1, boxCoords._2), (boxCoords._1 - 1, boxCoords._2 + 1), (boxCoords._1, boxCoords._2 + 1),
            (boxCoords._1 + 1, boxCoords._2 + 1), (boxCoords._1 + 1, boxCoords._2), (boxCoords._1 + 1, boxCoords._2 - 1), (boxCoords._1, boxCoords._2 - 1))
            .filter(coord => coord._1 >= 0 && coord._1 < n && coord._2 >= 0 && coord._2 < n)
            //            .distinct
            .map(coord => computeIndex(n, coord))
    }

    //    def main(args: Array[String]): Unit = {
    //
    //        val n = 8
    //
    //        //        (n - 1 to 0 by -1).foreach(row => {
    //        //
    //        //            print("%3d=> ".format(row))
    //        //
    //        //            (0 until n).foreach(col => print("| %3d ".format(computeIndex(n, (col, row)))))
    //        //
    //        //            println("|")
    //        //        })
    //        //
    //        //        (0 until (n * n)).foreach(hIdx => println("%d -> %s".format(hIdx, reverseIndex(n, hIdx))))
    //
    //        (0 until (n * n)).foreach(hIdx => println("%d -> %s".format(hIdx, getNearbyIndexes(n, hIdx))))
    //    }

    private def rotate(n: Long, x: RefLong, y: RefLong, rx: Boolean, ry: Boolean) {

        if (!ry) {
            if (rx) {
                x.i = n - 1 - x.i
                y.i = n - 1 - y.i
            }

            //Swap x and y
            val t = x.i
            x.i = y.i
            y.i = t
        }
    }

    /**
      * A bit operation mapping from a Hilbert box coordinates ( (0,0)...(n-1, n-1)) to a Hilbert index (0...n-1)
      */
    private def mapToIndex(n: Long, x: RefLong, y: RefLong) = {

        var rx = false
        var ry = false
        var d = 0L
        var s = n >>> 1 // n/2

        while (s > 0) {

            rx = (x.i & s) != 0
            ry = (y.i & s) != 0

            d += s * s * ((if (rx) 3 else 0) ^ (if (ry) 1 else 0))

            rotate(s, x, y, rx, ry)

            s >>>= 1 // s/=2
        }

        d
    }
}