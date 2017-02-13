package com.bl.bd.presentation

import java.io.FileOutputStream

import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.ss.usermodel.CellType

import scala.io.Source

/**
  * Created by MK33 on 2016/12/9.
  */
object OutputExcelItemBased {

  def main(args: Array[String]) {

    val shopName = "华联超市浙中店"
    val wb = new HSSFWorkbook()
    val sheet = wb.createSheet(shopName)
    val files = Source.fromFile("F:\\download\\chrome\\part-00000 (1)").getLines()
    files.foreach { line =>
      val words = line.split("\t")
      if (words(0).equals(shopName)) {
        var i = 0
        words(1).split(",").map { s =>
          val l = s.split(":")
          val row = sheet.createRow(i)
          val cell0 = row.createCell(0)
          val cell1 = row.createCell(1)
          val cell2 = row.createCell(2)
          val cell3 = row.createCell(3)
          cell3.setCellType(CellType.NUMERIC)
          cell0.setCellValue(shopName)
          cell1.setCellValue(l(0))
          cell2.setCellValue(l(1))
          cell3.setCellValue(l(2).toDouble)
          i += 1
        }
      }

    }

    val fileOut = new FileOutputStream( "F:\\item_based_" + shopName + ".xls")
    wb.write(fileOut)
    wb.close()
    fileOut.close()


  }

}
