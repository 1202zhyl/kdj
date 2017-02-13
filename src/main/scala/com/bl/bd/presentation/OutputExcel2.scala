package com.bl.bd.presentation

import java.io.{FileInputStream, FileOutputStream}

import com.bl.bd.dao.StoreHotSaleGoodsRating
import com.bl.bd.util.SparkFactory
import org.apache.poi.hssf.usermodel.{HSSFRow, HSSFWorkbook}
import org.apache.poi.ss.usermodel.{CellStyle, CellType, Row, WorkbookFactory}
import org.apache.poi.xssf.usermodel.{XSSFRow, XSSFWorkbook}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by MK33 on 2016/12/14.
  */
object OutputExcel2 {

  def main(args: Array[String]) {

    /**
      * translate("F:\\download\\chrome\\part-00000 (3)", "浙中路店.xls", (line: String, row: Row) => {
      * val cell0 = row.createCell(0)
      * val cell1 = row.createCell(1)
      * val cell2 = row.createCell(2)
      * val cell3 = row.createCell(3)
      * val cell4 = row.createCell(4)
      * cell4.setCellType(CellType.NUMERIC)
      * val words = line.split(",")
      * cell0.setCellValue(words(0))
      * cell1.setCellValue(words(1))
      * cell2.setCellValue(words(2))
      * cell3.setCellValue(words(3))
      * cell4.setCellValue(words(4).toDouble)
      * })
      */

    if (args.length < 6) {
      println("error parameter")
      printUsage
      sys.exit(1)
    }
    var store: String = null
    var goodsType: String = null
    var goodsList: String = null

    for (i <- 0 until args.length if i % 2 == 0) {
      args(i) match {
        case "--store" => store = args(i + 1)
        case "--goods_type" => goodsType = args(i + 1)
        case "--goods_list" => goodsList = args(i + 1)
        case e => println("don't know option: " + e)
          sys.exit(1)
      }
    }
//    selectGoods("华联超市浙中店", "基础型")

    selectGoods(store, goodsType, goodsList)


  }

  def translate(input: String, output: String, f: (String, Row) => Unit): Unit = {
    val wb = new HSSFWorkbook()
    val sheet = wb.createSheet("new sheet")
    var i = 0
    Source.fromFile(input).getLines().foreach { line =>
      val row = sheet.createRow(i)
      f(line, row)
      i += 1
    }
    val fileOut = new FileOutputStream(output)
    wb.write(fileOut)
    wb.close()
    fileOut.close()

  }


  def selectGoods(storeName: String, goodsType: String, goodsList: String): Unit = {
    // 得到商品得分排行榜
//    val goodsAndPref = StoreHotSaleGoodsRating.getStoreHotSale(storeName)
    val goodsAndPref = Choose.getStoreGoodsRating(storeName).map(s => (s._3, s._5)).sortWith(_._2 > _._2)
    // 每个品类已经选的商品数 map
    val categoryCountMap = mutable.Map.empty[String, Int]
    // 已选商品 map
    val chooseGoodsMap = mutable.Map.empty[String, Double]
    // 商品到一级类目映射 map
    val goods2CategoryMap = mutable.Map.empty[String, String]
    // 加载门店商品数据
    val storeGoodsFile = new FileInputStream(goodsList)
    val wb = WorkbookFactory.create(storeGoodsFile)
    val sheet = wb.getSheet(goodsType)
    val rows = sheet.rowIterator()
    val headRow = rows.next()
    val cells = headRow.cellIterator().toArray.map(_.getStringCellValue)
    val oneLevel = cells.indexOf("一级类目")
    val goodsCodeIndex = cells.indexOf("商品编码")

    while (rows.hasNext) {
      val row = rows.next()
      val firstLevel = row.getCell(oneLevel).getStringCellValue
      if (!categoryCountMap.contains(firstLevel)) {
        categoryCountMap(firstLevel) = 0
      }
      val goodsCode = row.getCell(goodsCodeIndex)
      goodsCode.setCellType(CellType.STRING)
      goods2CategoryMap(goodsCode.getStringCellValue) = firstLevel
    }

    // 第一轮：每个一级品类挑选10个商品
    val initSize = 10 * categoryCountMap.size
    var i, j = 0
    val goodsCount = goodsAndPref.length
    while ( i < goodsCount && j < initSize) {
      val goods = goodsAndPref.toSeq.get(i)
      if (!categoryCountMap.contains(goods._1) || categoryCountMap(goods._1) < 10) {
        chooseGoodsMap += goods
        categoryCountMap(goods._1) += 1
        j += 1
      }
      i += 1
    }



    // 第二轮：热度从大到小加入map，直到满1200为止
    val totalSize = 1200
    val firstSize = chooseGoodsMap.size
    val delt = totalSize - firstSize
    i = 0
    var c = 0
    while ( i < goodsCount && c < delt) {
      val goods = goodsAndPref(i)
      if (!chooseGoodsMap.contains(goods._1)) {
        chooseGoodsMap += goods
        c += 1
      }
      i += 1
    }

    // 第三轮：将选择的商品从商品类别中挑选出来，生成另一个excel商品列表
    val outputExcel = new XSSFWorkbook()
    val outputSheet = outputExcel.createSheet("new sheet1")
    val chooseSheet = sheet.rowIterator()
    val outHeader = outputSheet.createRow(0)
    val headTmp = chooseSheet.next()
    copyRow(headTmp, outHeader)
    outHeader.createCell(headTmp.getLastCellNum).setCellValue("得分")

    i = 1
    for (row <- chooseSheet if chooseGoodsMap.contains(row.getCell(goodsCodeIndex).getStringCellValue)) {
      val tmpRow = outputSheet.createRow(i)
      val goodsAndScore = chooseGoodsMap(row.getCell(goodsCodeIndex).getStringCellValue)
      copyRow(row, tmpRow, goodsAndScore)
      i += 1
    }
    println(i)
    val fileOutputStream = new FileOutputStream(storeName + ".xlsx")
    outputExcel.write(fileOutputStream)
    outputExcel.close()
    storeGoodsFile.close()
  }


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

  def printUsage = {
    println(
      s"""
        |Usage: ${getClass.getSimpleName} --store <store name> --goods_type <shop goods type>  --goods_list <goodsList excel file>
        |    --store: store name
        |    --goods_type:  goods type
        |    --goods_list: excel file, end with .xlsx
      """.stripMargin)

  }

  val sc = SparkFactory.getSparkContext
  val goodsAndPref = sc.textFile("/tmp/kdj/result/华联超市浙中店").map { s =>
    val a = s.split(",")
    (a(2), a(4).toDouble)
  }.collect().sortWith(_._2 > _._2).toSeq

}
