package com.bl.bd

import com.bl.bd.rating.PriceAndAmtRating

/**
  * 计算商品相似度，然后保存在 hdfs 和 hbase 中
  * Created by MK33 on 2016/12/16.
  */
object StoreGoodsRatingCalculator {

  def main(args: Array[String]) {
    run("/tmp/kdj2", "shop_goods_rating")
  }

  def run(outputRootPath: String, hbaseTable: String): Unit = {
    PriceAndAmtRating.run(outputRootPath, hbaseTable)
  }

}
