package org.cusp.bdi.util

import scala.collection.mutable.ListBuffer

//object InputFileParsers extends InputFileParsers {}

object InputFileParsers extends Serializable {

    def nycLION = (line: String) => {

        try {
            val idx0 = line.indexOf(',')
            val streetID = line.substring(0, idx0)

            val lineCoords = line.substring(idx0 + 20, line.length() - 3)

            val coordArr = lineCoords.split(',')
                .map(row => {

                    val arr = row.split(' ')

                    (arr(0), arr(1))
                })

            (streetID, coordArr)
        }
        catch {
            case ex: Exception => {

                ex.printStackTrace()
                null
            }
        }
    }

    def nycLION_WGS84 = (line: String) =>
        nycLION(line)

    def nycLION_Segments = (line: String) => {

        val (streetID, coordArr) = nycLION(line)

        if (streetID == null)
            null
        else {

            val lst = ListBuffer[(String, ((String, String), (String, String)))]()

            var startPoint = coordArr(0)

            (1 until coordArr.length).foreach(i => {

                val endPoint = coordArr(i)

                lst.append((streetID, ((startPoint, endPoint))))

                startPoint = endPoint
            })

            lst.toArray
        }
    }

    def tpepPoints = (line: String) => {

        val xy = getXY(line, 2 /*, false*/ )

        if (xy == null)
            null
        else
            (line.toLowerCase, xy)
    }

    def tpepPoints_WGS84 = (line: String) => {

        val xy = getXY(line, 2 /*, false*/ )

        if (xy == null)
            null
        else
            (line.toLowerCase, xy)
    }

    def taxiPoints = (line: String) => {

        val xy = getXY(line, 5)

        if (xy == null || xy._1.contains('-') || xy._2.contains('-'))
            null
        else
            (line.toLowerCase, xy)
    }

    def threePartLine = (line: String) => {

        val parts = line.split(",")

        if (parts(1).charAt(0) == '-' || parts(2).charAt(0) == '-')
            null
        else
            (parts(0), (parts(1), parts(2)))
    }

    def taxiPoints_WGS84 = (line: String) => {

        val xy = getXY(line, 5 /*, false*/ )

        if (xy == null)
            null
        else
            (line.toLowerCase, xy)
    }

    def busPoints = (line: String) => {

        val xy = getXY(line, 1 /*, true*/ )

        if (xy == null)
            null
        else
            (line.toLowerCase, xy)
    }

    def busPoints_WGS84 = (line: String) => {

        val xy = getXY(line, 1 /*, false*/ )

        if (xy == null)
            null
        else
            (line, (xy._2, xy._1)) // lon/lat reversed in data source
    }

    def osmPoints_WGS84 = (line: String) =>
        getXY_NoLoad(line)

    def randPoints = (line: String) => {

        val xy = line.split(',')

        (xy(0), (xy(1), xy(2)))
    }

    //    def osmBuildings = (line: String) => {
    //
    //        if (line.length() > 10) {
    //
    //            val idx = Helper.indexOf(line, ",")
    //
    //            val id = line.substring(0, idx)
    //            val coordStr = line.substring(idx + 11, line.length() - 3)
    //
    //            val coordArr = ListBuffer[(Int, Int)]()
    //
    //            coordStr.split(",")
    //                .map(x => parseXY(x, ' '))
    //                .map(x => coordArr += x)
    //
    //            if (coordArr.head != coordArr.last)
    //                coordArr += coordArr.head
    //
    //            while (coordArr.length < 4)
    //                coordArr += coordArr.head
    //
    //            (line, coordArr.toArray)
    //        }
    //        else
    //            null
    //    }

    //    def keyBus = (line: String) => {
    //
    //        val idx = Helper.indexOf(line, ",", 11)
    //
    //        //         (String,List[String])
    //        if (idx != -1)
    //            (line.substring(0, idx), line.substring(idx + 1).split(','))
    //        else
    //            (line, null)
    //    }

    private def getXY_NoLoad(line: String) =
        try {

            val xy = line.split(',')

            if (xy.length != 2)
                null
            else
                (line, (xy.head, xy.last))
        }
        catch {
            case ex: Exception => {

                ex.printStackTrace()
                null
            }
        }

    private def getXY(line: String, startCommaNum: Int /*, removeDecimal: Boolean*/ ) = {

        def getCommaPos(commaNum: Int, startIdx: Int) = {

            var count = 0
            var idx = startIdx

            while (count < commaNum) {

                if (line(idx) == ',')
                    count += 1

                idx += 1
            }

            idx
        }

        try {

            val idx0 = getCommaPos(startCommaNum, 0)
            val idx1 = getCommaPos(1, idx0 + 1)
            val idx2 = getCommaPos(1, idx1 + 1)

            var x = line.substring(idx0, idx1 - 1)
            var y = line.substring(idx1, idx2 - 1)

            //            if (removeDecimal) {
            //
            //                x = x.substring(0, x.indexOf('.'))
            //                y = y.substring(0, y.indexOf('.'))
            //            }

            if (x(0) != '-' && (x(0) < '0' || x(0) > '9'))
                null
            else
                (x, y)
        }
        catch {
            case ex: Exception => {

                ex.printStackTrace()
                null
            }
        }
    }
}