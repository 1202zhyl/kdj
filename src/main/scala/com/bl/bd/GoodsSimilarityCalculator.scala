package com.bl.bd

import com.bl.bd.algorithm.{ItemBaseRecommend, ItemSimilarityMahout}

/**
  * 根据 (user, item, rating) 计算商品相似度
  * Created by MK33 on 2016/12/16.
  */
object GoodsSimilarityCalculator {

  def main(args: Array[String]) {

    var cmd =
      """
        |/opt/spark/apache-mahout-distribution-0.12.2/bin/mahout itemsimilarity
        |--input /tmp/kdj2/shop_goods_rating_item_based_mahout
        |--output /tmp/kdj2/goods_similarity/item_based_mahout
        |--similarityClassname SIMILARITY_LOGLIKELIHOOD
        |--tempDir /tmp/kdj2/mahout-tmp
        |
      """.stripMargin

    var userItemRating = "/tmp/kdj2/shop_goods_rating"
    var hBaseTable = "shop_goods_similarity"
    var keyPrefix = "shit_"

    for (i <- 0 until args.length if i % 2 == 0) {
      args(i) match {
        case "--mahout-cmd" => cmd = args(i + 1)
        case "--input" => userItemRating = args(i + 1)
        case "--hbase" => hBaseTable = args(i + 1)
        case "--key-prefix" => keyPrefix = args(i + 1)
        case e => println("don't know option: " + e)
          sys.exit(1)
      }
    }

    run(userItemRating, cmd, hBaseTable, keyPrefix)

  }


  def run(userItemRating: String, cmd: String, hBaseTable: String, hbseKeyPrefix: String): Unit = {
    ItemSimilarityMahout.train(userItemRating, cmd, hBaseTable, hbseKeyPrefix)
  }

}
