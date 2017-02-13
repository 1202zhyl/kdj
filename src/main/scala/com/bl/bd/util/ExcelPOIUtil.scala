package com.bl.bd.util

import org.apache.poi.ss.usermodel.{CellType, Row}
import scala.collection.JavaConversions._

/**
  * Created by MK33 on 2016/12/19.
  */
object ExcelPOIUtil {

  def copyRow(fromRow: Row, toRow: Row) = {
    fromRow.cellIterator().foreach { cell =>
      val cell2 = toRow.createCell(cell.getColumnIndex)
      cell2.setCellStyle(cell2.getCellStyle)
      val cellType = cell.getCellTypeEnum
      cellType match {
        case CellType.BOOLEAN => cell2.setCellValue(cell.getBooleanCellValue)
        case CellType.NUMERIC => cell2.setCellValue(cell.getNumericCellValue)
        case CellType.STRING => cell2.setCellValue(cell.getStringCellValue)
        case CellType.FORMULA => cell2.setCellValue(cell.getCellFormula)
        case CellType.BLANK => cell2.setCellValue(cell.getStringCellValue)
        case e =>
      }
    }
  }
  def copyRow(fromRow: Row, toRow: Row, newCellValue: Double) = {
    var c = 0
    fromRow.cellIterator().foreach { cell =>
      c += 1
      val cell2 = toRow.createCell(cell.getColumnIndex)
      cell2.setCellStyle(cell2.getCellStyle)
      val cellType = cell.getCellTypeEnum
      cellType match {
        case CellType.BOOLEAN => cell2.setCellValue(cell.getBooleanCellValue)
        case CellType.NUMERIC => cell2.setCellValue(cell.getNumericCellValue)
        case CellType.STRING => cell2.setCellValue(cell.getStringCellValue)
        case CellType.FORMULA => cell2.setCellValue(cell.getCellFormula)
        case CellType.BLANK => cell2.setCellValue(cell.getStringCellValue)
        case e =>
      }
    }
    val cell = toRow.createCell(c)
    cell.setCellType(CellType.NUMERIC)
    cell.setCellValue(newCellValue)
  }
}
