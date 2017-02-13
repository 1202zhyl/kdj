package com.bl.bd

import java.io.FileOutputStream

import org.apache.poi.hssf.usermodel.HSSFWorkbook

import scala.io.Source

/**
  * Created by MK33 on 2016/12/9.
  */
object GGG {

  var input: String = _
  var shopName: String = _
  var outputT: String = _

  def main(args: Array[String]) {
    if (args.length < 2) {
      printHelper
    }

    for (i <- 0 until args.length if i % 2 == 0) {
      args(i) match {
        case "--input" => input = args(i + 1)
        case "--shop_name" => shopName = args(i + 1)
        case "--output" => outputT = args(i + 1)
        case _ =>
          println(s"don't know option: ${args(i)}")
          sys.exit(-1)
      }
    }

    if (input == null) {
      println("cannot find --input option")
      printHelper
      sys.exit(-1)
    }
    if (shopName == null) {
      println("cannot find --shop_name option")
      printHelper
      sys.exit(-1)
    }

    val output = if (outputT == null) "." else outputT
    val shopRegex = shopName.r


    val files = Source.fromFile(input).getLines()

    files.foreach { line =>
      val words = line.split("\t")
      if (shopRegex.pattern.matcher(words(0)).matches()) {
        val wb = new HSSFWorkbook()
        val sheet = wb.createSheet(shopName)
        var i = 0
        words(1).split(",").map { s =>
          val l = s.split(":")
          val row = sheet.createRow(i)
          val cell0 = row.createCell(0)
          val cell1 = row.createCell(1)
          val cell2 = row.createCell(2)
          val cell3 = row.createCell(3)
          cell0.setCellValue(shopName)
          cell1.setCellValue(l(0))
          cell2.setCellValue(l(1))
          cell3.setCellValue(l(2).toDouble)
          i += 1
        }
        val fileOut = new FileOutputStream(output + "/" + words(0) + ".xls")
        wb.write(fileOut)
        wb.close()
        fileOut.close()
      }

    }


  }

  def printHelper = {
    println(
      """
        |Usage: --input <file name> --shop_name <shop name>  --output <result dir> --help
        |  --input: input file path and name, format: shopName<tab>goodsCode0:goodsName0:rating0,goodsCode1:goodsName1:rating1,......
        |  --shop_name: shop name regex
        |  --dir: where to save the final result, default the dir contains this jar
        |  --help: println this help information
      """.stripMargin)
  }


}
