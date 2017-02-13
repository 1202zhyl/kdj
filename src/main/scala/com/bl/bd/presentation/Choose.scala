package com.bl.bd.presentation

import com.bl.bd.util.{Configuration, SparkFactory}
import org.apache.hadoop.fs.Path

/**
  * Created by MK33 on 2016/12/14.
  */
object Choose {

  def main(args: Array[String]) {

    getStoreGoodsRating("华联超市浙中店")

  }

  // 得到某个商店周围商品的排行榜 (goods_sid, goods_name, mdm_goods_id, store, rating)
  def getStoreGoodsRating(storeName: String): Seq[(String, String, String, String, Double)] = {

    val sc = SparkFactory.getSparkContext
    val spark = SparkFactory.getSparkSession()


    import spark.implicits._
    val itemSimilarityRawRDD = sc.textFile("/tmp/kdj/item_similarity_item_based_final").map(s => {
      val l = s.split("\t")
      (l(0), (l(1), l(2).toDouble))
    })

    // 商品之间的相似度
    val itemSimilarityRDD = itemSimilarityRawRDD.map(s => (s._2._1, (s._1, s._2._2))) ++ itemSimilarityRawRDD
    itemSimilarityRDD.map(s => (s._1, s._2._1, s._2._2)).toDF("goods1", "goods2", "similarity").createOrReplaceTempView("goods_similarity")

    // 加载门店商品
    val storeRawRDD = sc.sequenceFile[String, String]("/tmp/kdj/lh")
    storeRawRDD.toDF("goods_code", "goods_name").createOrReplaceTempView("store_goods")
    val storeRDD = spark.sql(
      """
        |select distinct g.sid, s.goods_name, g.mdm_goods_sid
        |from  recommendation.goods_avaialbe_for_sale_channel g join  store_goods s on s.goods_code = g.mdm_goods_sid
        |
      """.stripMargin).rdd.map(row => (row.getString(0), (row.getString(1), row.getString(2))))

    // 加载门店周围商品的热度排行, (goods, (store, pref))
    val storeAroundGoods = sc.textFile("/tmp/kdj/shop_goods_rating_item_based").map { line =>
      val l = line.split("\t")
      (l(1), (l(0), l(2).toDouble))
    }.filter(_._2._1 == storeName)

    storeAroundGoods.map(s => (s._1, s._2._1, s._2._2)).toDF("goods", "store", "pref").createOrReplaceTempView("shop_around_goods")

    // 找到和热销商品相似的，但是并不在热销商品内的哪些商品
    val itemSimReduceStore = itemSimilarityRDD.subtractByKey(storeAroundGoods).map(s => (s._2._1, (s._1, s._2._2)))
    val sim = storeAroundGoods.join(itemSimReduceStore).flatMap { case (goods1, ((store, pref), (goods2, similarity))) =>
      Array((goods1, (store, pref)), (goods2, (store, pref * similarity)))
    }

    // 挑选出门店卖的商品
    val goodsFinal = sim.join(storeRDD).map { case (goods, ((store, pref), goodsName)) =>
      (goods, goodsName._1, goodsName._2, store, pref)
    }.groupBy(_._1).map(s => s._2.maxBy(_._4)).sortBy(_._4)

    val fs = Configuration.getHadoopFS
    if (fs.exists(new Path("/tmp/kdj/result/" + storeName))) {
      fs.delete(new Path("/tmp/kdj/result/" + storeName), true)
    }
    goodsFinal.map(s => s._1 + "," + s._2.replace(",", "") + "," + s._3 + "," + s._4 + "," + s._5).
      coalesce(1).saveAsTextFile("/tmp/kdj/result/" + storeName)

    goodsFinal.collect()

  }








}
