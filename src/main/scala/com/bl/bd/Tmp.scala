package com.bl.bd

import java.io.FileInputStream

import org.apache.poi.ss.usermodel.WorkbookFactory

/**
  * Created by MK33 on 2017/1/18.
  */
object Tmp {

  def main(args: Array[String]) {

    println("hhh")
    val file = "F:\\personal\\QQ\\819307659\\FileRecv\\member_info表结构及字段说明.xlsx"
    val inputStream = new FileInputStream(file)
    val wb = WorkbookFactory.create(inputStream)
    val sheet = wb.getSheetAt(0)

    val rows = sheet.rowIterator()
    while (rows.hasNext) {
      val cells = rows.next().cellIterator()
      val array = new Array[String](3)
      var i = 0
      while (cells.hasNext) {
        val cell = cells.next().getStringCellValue
        array(i) = cell
        i += 1
      }
      println(array(1) + "=" + array(2) + ":" + array(0))
    }




  }

}
