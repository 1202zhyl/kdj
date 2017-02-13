package com.bl.bd

import java.io.{FileInputStream, FileOutputStream}

import com.bl.bd.util.{ExcelPOIUtil, SparkFactory}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.poi.ss.usermodel.{CellType, WorkbookFactory}
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by MK33 on 2016/12/19.
  */

case class GoodsSimilarity(goods_one: String, goods_two: String, similarity: Double)

object GetGoodsToBeSell {

  def main(args: Array[String]) {

    var excel: String = "世纪联华到家ABC门店及商品清单1028.xlsx"
    var sheetName: String = "基础型"
    var storeName: String = "联华超市新二店"
    var totalNum: Int = 1200

    for (i <- 0 until args.length if i % 2 == 0) {
      args(i) match {
        case "--excel" => excel = args(i + 1)
        case "--sheet" => sheetName = args(i + 1)
        case "--store" => storeName = args(i + 1)
        case "--num" => totalNum = args(i + 1).toInt
        case e =>
          println("don't know option: " + e)
          sys.exit(-1)
      }
    }

    require(excel != null && sheetName != null && storeName != null )

    val sc = SparkFactory.getSparkContext
    val spark = SparkFactory.getSparkSession()
    import spark.implicits._
    // 得到门店周围商品排行榜
//    val storeGoodsRating = getHBaseStoreGoodsRating(sc, "shop_goods_rating", "info", "rating", storeName + "_")
    val storeGoodsRating = getHDFSStoreGoodsRating(sc, "/tmp/kdj2/shop_goods_rating", storeName)
    // 得到物品之间的相似度
//    val rawGoodsSimilarity = getHBaseSimilarity(sc, "shop_goods_similarity", "info", "most_like_goods", "shit_")
    val rawGoodsSimilarity = getSimilarity(sc, "/tmp/kdj2/goods_similarity/item_based_mahout_final")
    // 得到门店周围商品的综合排行榜
    val finalRanking = getMostSimilarityGoods(storeGoodsRating, rawGoodsSimilarity)
    finalRanking.toDF("goods_id", "ranking").createOrReplaceTempView("goods_ranking")
    val fin = spark.sql(
      """
        |select distinct g.mdm_goods_sid, r.ranking
        |from goods_ranking r
        |join recommendation.goods_avaialbe_for_sale_channel g on g.sid = r.goods_id
      """.stripMargin).rdd.map(r => (r.getString(0), r.getDouble(1))).collect()

    getSelectedStoreGoodsList(excel, sheetName, fin, totalNum, storeName)


  }

  /**
    * 得到综合排行榜
    * @param goodsRating  商店对物品的打分
    * @param goodsSimilarity 商品之间的相似度
    * @return
    */
  def getMostSimilarityGoods(goodsRating: RDD[(String, Double)], goodsSimilarity: RDD[(String, (String, Double))]): RDD[(String, Double)] = {

    val allGoods = goodsRating.join(goodsSimilarity).flatMap { case (goodsOne, (rating, (goodsSimilarity, similarity))) =>
        Array((goodsOne, rating), (goodsSimilarity, rating * similarity))
    }.reduceByKey(math.max)
    allGoods
  }


  def getSelectedStoreGoodsList(excel: String, sheetName: String, fin: Seq[(String, Double)], totalNum: Int, storeName: String): Unit = {
    // 每个品类已经选的商品数 map
    val categorySelectedCountMap = mutable.Map.empty[String, Int]
    // 已选商品 map
    val selectedGoodsMap = mutable.Map.empty[String, Double]
    // 商品到一级类目映射 map
    val goods2CategoryMap = mutable.Map.empty[String, String]
    // 加载门店商品数据
    val storeGoodsFile = new FileInputStream(excel)
    val wb = WorkbookFactory.create(storeGoodsFile)
    val sheet = wb.getSheet(sheetName)
    val rows = sheet.rowIterator()
    val headRow = rows.next()
    val cells = headRow.cellIterator().toArray.map(_.getStringCellValue)
    val oneLevel = cells.indexOf("一级类目") // 得到一级类目的列索引
    val goodsCodeIndex = cells.indexOf("商品编码") // 得到商品编码的列索引

    // 初始化 categorySelectedCountMap 和 goods2CategoryMap
    while (rows.hasNext) {
      val row = rows.next()
      val firstLevel = row.getCell(oneLevel).getStringCellValue
      if (!categorySelectedCountMap.contains(firstLevel)) {
        categorySelectedCountMap(firstLevel) = 0
      }
      val goodsCode = row.getCell(goodsCodeIndex)
      goodsCode.setCellType(CellType.STRING)
      goods2CategoryMap(goodsCode.getStringCellValue) = firstLevel
    }
    // 第一轮：每个一级品类挑选10个商品
    val initSize = 10 * categorySelectedCountMap.size
    var i, j = 0
    val goodsCount = fin.length
    while ( i < goodsCount && j < initSize) {
      val goods = fin(i)
      if (goods2CategoryMap.contains(goods._1)) {
        val category = goods2CategoryMap(goods._1)
        if (categorySelectedCountMap.getOrElse(category, 0) < 10) {
          selectedGoodsMap += goods
          categorySelectedCountMap(goods._1) = categorySelectedCountMap.getOrElse(goods._1, 0) + 1
          j += 1
        }
      }
      i += 1
    }


    // 第二轮：热度从大到小加入map，直到满1200为止
    val totalSize = totalNum
    val firstSize = selectedGoodsMap.size
    val delt = totalSize - firstSize
    i = 0
    var c = 0
    while ( i < goodsCount && c < delt) {
      val goods = fin(i)
      if (!selectedGoodsMap.contains(goods._1) && goods2CategoryMap.contains(goods._1)) {
        selectedGoodsMap += goods
        c += 1
      }
      i += 1
    }

    // 第三轮：将选择的商品从商品类别中挑选出来，生成另一个excel商品列表
    val outputExcel = new XSSFWorkbook()
    val outputSheet = outputExcel.createSheet(sheetName)
    val chooseSheet = sheet.rowIterator()
    val outHeader = outputSheet.createRow(0)
    val headTmp = chooseSheet.next()
    ExcelPOIUtil.copyRow(headTmp, outHeader)
    outHeader.createCell(headTmp.getLastCellNum).setCellValue("得分")

    i = 1
    for (row <- chooseSheet if selectedGoodsMap.contains(row.getCell(goodsCodeIndex).getStringCellValue)) {
      val tmpRow = outputSheet.createRow(i)
      val goodsCodeCell = row.getCell(goodsCodeIndex)
      goodsCodeCell.setCellType(CellType.STRING)
      val goodsAndScore = selectedGoodsMap(goodsCodeCell.getStringCellValue)
      ExcelPOIUtil.copyRow(row, tmpRow, goodsAndScore)
      i += 1
    }

    val fileOutputStream = new FileOutputStream(storeName + ".xlsx")
    outputExcel.write(fileOutputStream)
    outputExcel.close()
    storeGoodsFile.close()

  }

  /** 得到商品之间的相似度 */
  def getSimilarity(sc: SparkContext, path: String): RDD[(String, (String, Double))] = {

    val goodsSimilarity = sc.textFile(path).flatMap { line =>
      val a = line.split("\t")
      a(1).split(",").map { s =>
        val b = s.split(":")
        (a(0).split(",")(0), (b(0), b(2).toDouble))
      }
    }

    (goodsSimilarity.map(s => (s._2._1, (s._1, s._2._2))) ++ goodsSimilarity).distinct()

  }


  /** 从 HBase 中读取物品之间的相似度 */
  def getHBaseSimilarity(sc: SparkContext, table: String, columnFamily: String, column: String, keyPrefix: String): RDD[(String, (String, Double))] = {

    val hBaseConfiguration = HBaseConfiguration.create()
    hBaseConfiguration.set(TableInputFormat.INPUT_TABLE, table)
    hBaseConfiguration.set("hbase.zookeeper.quorum", "slave23.bl.bigdata,slave24.bl.bigdata,slave21.bl.bigdata,slave22.bl.bigdata,slave25.bl.bigdata")

    val hbaseRawRDD = sc.newAPIHadoopRDD(hBaseConfiguration, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result]).map(_._2)

    val columnFamilyBytes = Bytes.toBytes(columnFamily)
    val columnBytes = Bytes.toBytes(column)

    val goodsSimilarityRDD = hbaseRawRDD.map{ row =>
      val key = Bytes.toString(row.getRow)
      if (!key.startsWith(keyPrefix)) {
        null
      } else {
        val mostLikeGoods = row.getValue(columnFamilyBytes, columnBytes)
        if (mostLikeGoods == null) null else {
          Bytes.toString(mostLikeGoods).split(",").map { s =>
            val g = s.split(":")
            (key.replace(keyPrefix, ""), (g(0), g(2).toDouble))
          }
        }
      }
    }.filter(_ != null).flatMap(s => s)

    goodsSimilarityRDD

  }

  def getHBaseStoreGoodsRating(sc: SparkContext, table: String, columnFamily: String, column: String, keyPrefix: String): RDD[(String, Double)] = {
    val hBaseConfiguration = HBaseConfiguration.create()
    hBaseConfiguration.set(TableInputFormat.INPUT_TABLE, table)
    hBaseConfiguration.set("hbase.zookeeper.quorum", "slave23.bl.bigdata,slave24.bl.bigdata,slave21.bl.bigdata,slave22.bl.bigdata,slave25.bl.bigdata")

    val hbaseRawRDD = sc.newAPIHadoopRDD(hBaseConfiguration, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result]).map(_._2)

    val columnFamilyBytes = Bytes.toBytes(columnFamily)
    val columnBytes = Bytes.toBytes(column)

    val goodsSimilarityRDD = hbaseRawRDD.map{ row =>
      val key = Bytes.toString(row.getRow)
      if (!key.startsWith(keyPrefix)) {
        null
      } else {
        val goodsPref = row.getValue(columnFamilyBytes, columnBytes)
        if (goodsPref == null) null else {
            (key.replace(keyPrefix, ""), Bytes.toDouble(goodsPref))
        }
      }
    }.filter(_ != null)

    goodsSimilarityRDD
  }

  def getHDFSStoreGoodsRating(sc: SparkContext, path: String, store: String): RDD[(String, Double)] = {
    val r = sc.textFile(path).map { line =>
      val a = line.split("\t")
      (a(0), a(1), a(2).toDouble)
    }.filter(_._1 == store).map(s => (s._2, s._3))
    r
  }



}
