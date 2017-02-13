package com.bl.bd.dao

import java.io.FileInputStream

import org.apache.poi.ss.usermodel.{CellType, WorkbookFactory}

import scala.collection.mutable.ListBuffer

/**
  * Created by MK33 on 2016/12/15.
  */
object StoreHotSaleGoodsRating {

  /** 根据门店名称得到周围热门商品排行 */
  def getStoreHotSale(storeName: String):  List[(String, Double)]  = {
    val hotSaleGoodsFile = new FileInputStream(storeName)
    val hotSaleWb = WorkbookFactory.create(hotSaleGoodsFile)
    val hotSaleSheet = hotSaleWb.getSheetAt(0)
    val hotSaleRows = hotSaleSheet.rowIterator()
    val hotSaleGoods = new ListBuffer[(String, Double)]()
    while (hotSaleRows.hasNext) {
      val row = hotSaleRows.next()
      val goodsCode = row.getCell(2)
      goodsCode.setCellType(CellType.STRING)
      val goodsCodeString = goodsCode.getStringCellValue
      val goodsPref = row.getCell(4).getNumericCellValue
      hotSaleGoods += ((goodsCodeString, goodsPref))
    }
    hotSaleGoods.toList
  }

}
