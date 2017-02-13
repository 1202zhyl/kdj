package com.bl.bd.presentation

import java.io.{BufferedReader, FileInputStream, FileOutputStream, InputStreamReader}

import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.ss.usermodel.{CellStyle, CellType}

import scala.io.Source

/**
  * Created by MK33 on 2016/12/9.
  */
object OutputExcel {

  def main(args: Array[String]) {

    val wb = new HSSFWorkbook()
    val sheet = wb.createSheet("new sheet")

    var i = 0
    var fileName: String = null
    Source.fromFile("F:\\download\\chrome\\part-00000").getLines().foreach { line =>
      val row = sheet.createRow(i)
      val cell0 = row.createCell(0)
      val cell1 = row.createCell(1)
      val cell2 = row.createCell(2)
      val cell3 = row.createCell(3)
      cell3.setCellType(CellType.NUMERIC)
      val words = line.split("\t")
      fileName = words(0)
      cell0.setCellValue(words(0))
      cell1.setCellValue(words(1))
      cell2.setCellValue(words(2))
      cell3.setCellValue(words(3).toDouble)
      i += 1
    }

    val fileOut = new FileOutputStream("F:\\" + fileName + ".xls")
    wb.write(fileOut)
    wb.close()
    fileOut.close()

  }

}
