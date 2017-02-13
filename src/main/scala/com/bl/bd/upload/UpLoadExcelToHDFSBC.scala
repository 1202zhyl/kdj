package com.bl.bd.upload

import java.io.FileInputStream

import com.bl.bd.util.{Configuration, SparkFactory}
import org.apache.hadoop.fs.Path
import org.apache.poi.ss.usermodel.{CellType, WorkbookFactory}
import scala.collection.JavaConversions._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by MK33 on 2016/12/13.
  */
object UpLoadExcelToHDFSBC {

  object Excel {
    val goodsName = "产品名称"
    val goodsCode = "商品编码"
  }

  def main(args: Array[String]) {

    if (args.length < 4) {
      println("error parameter !")
      printUsage
      sys.exit(-1)
    }

    val (input, output) = parseArgs(args)

    val inputStream = new FileInputStream(input)
    val wb = WorkbookFactory.create(inputStream)
    val sheet = wb.getSheet("基础型清单")

    val rows = sheet.rowIterator()
    val header = rows.next().toArray.map(_.getStringCellValue)
    val goodsName = header.indexOf(Excel.goodsName)
    val goodsCode = header.indexOf(Excel.goodsCode)

    var i = 0
    val goods = for (row <- rows) yield {
      val cells = row.cellIterator()
      val ab = new ArrayBuffer[String]()
      var goodsCodeStr: String = null
      var goodsNameStr: String = null
      var j0 = 0
      var j1 = 0
      while (cells.hasNext) {
        val cell = cells.next()
        val cellType = cell.getCellTypeEnum()
        cellType match {
          case CellType.STRING => ab += cell.getStringCellValue
          case CellType.NUMERIC =>
            cell.setCellType(CellType.STRING)
            ab += cell.getStringCellValue
          case CellType.BOOLEAN => ab += cell.getBooleanCellValue.toString
          case CellType.BLANK => ab += "null"
          case CellType.ERROR =>
          case CellType._NONE =>
          case CellType.FORMULA =>
        }
        if (j0 == goodsCode) {
          goodsCodeStr = cell.getStringCellValue
        }
        if (j1 == goodsName) {
          goodsNameStr = cell.getStringCellValue
        }
        j0 += 1
        j1 += 1
      }
      i += 1
      (goodsCodeStr, goodsNameStr)
    }
//    Console println goods.mkString("\n")
    inputStream.close()
    wb.close()

    // 保存成 hdfs Sequence file
    val sc = SparkFactory.getSparkContext
    val fs = Configuration.getHadoopFS
    if (fs.exists(new Path(output))) {
      fs.delete(new Path(output), true)
    }
    sc.makeRDD(goods.toArray).saveAsSequenceFile(output)


  }


  def printUsage = {

    println(
      s"""
         |Usage: ${getClass.getSimpleName}: --input [local execl file] --output [HDFS path]
         |    --input | -i :  local execl file
         |    --output | -o : save to HDFS path
         |
      """.stripMargin)

  }


  /** 解析命令行参数 */
  def parseArgs(args: Array[String]): (String, String) = {
    var input: String = null
    var output: String = null
    for (i <- 0 until args.length if i % 2 == 0) {
      args(i) match {
        case "--input" => input = args(i + 1)
        case "-i" => input = args(i + 1)
        case "--output" => output = args(i + 1)
        case "-o" => output = args(i + 1)
        case e =>
          println("not known option: " + args(i))
          throw new IllegalArgumentException(e)
      }
    }
    require(input != null, "please input input file")
    require(output != null, "please input output file " +
      "")
    (input, output)
  }

}
