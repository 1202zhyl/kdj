package com.bl.bd.kdj

import com.bl.bd.recommend.{CollaborativeFilterItemBased, UserBasedRecommend}
import com.bl.bd.util.{KDJConfiguration, ShellUtil, SparkFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender
import org.apache.mahout.drivers.ItemSimilarityDriver
import org.apache.spark.sql.functions._

/**
  *
  * 基于用户和基于物品相似度入口
  * Created by MK33 on 2016/11/25.
  *
  */
object Main {

  def main(args: Array[String]) {

    val spark = SparkFactory.getSparkSession()

    val rootDir = "/tmp/kdj"
    val orderSql =
      """
        |SELECT od.order_no, od.goods_code, adr.storename, od.sale_price, od.sale_sum
        |FROM sourcedata.s03_oms_order_detail od
        |JOIN sourcedata.s03_oms_order_sub oa ON oa.order_no = od.order_no
        |JOIN address_coordinate_store adr ON adr.address = regexp_replace(oa.recept_address_detail, ' ', '')
        |
      """.stripMargin
    import spark.implicits._
    val orderRawRDD = spark.sql(orderSql).distinct()
    val a = orderRawRDD.select("goods_code", "storename", "sale_price", "sale_sum").
      groupBy("goods_code", "storename").agg(sum($"sale_sum" * $"sale_price") as "money", sum("sale_sum") as "amt")
    a.createOrReplaceTempView("spark_a")
    // 每个门店单品销售额的最大值和销售的最大数量
    val b = a.select("money", "amt", "storename").groupBy("storename").
      agg(max("money") as "max_money", min("money") as "min_money", max("amt") as "max_amt", min("amt") as "min_amt")

    b.createOrReplaceTempView("spark_b")

    spark.udf.register("my_normalization", (min: Double, max: Double, v: Double) => {if (min == max) -1.0 else 5.0 * (v - min) / (max - min)})

    val c = spark.sql(
      """
        |select a.storename, a.goods_code, my_normalization(b.min_money, b.max_money, a.money) money_score,
        |my_normalization(b.min_amt, b.max_amt, a.amt) amt_score
        |from spark_a a
        |join spark_b b on b.storename = a.storename
      """.stripMargin)

    c.createOrReplaceTempView("result")

    val userItemRatingRDD = spark.sql(
      """
        |select storename, goods_code, money_score + amt_score as rating
        |from result
      """.stripMargin)

    val ration = KDJConfiguration.getConf("store.most.interest.goods.ratio").getOrElse("0.5").toDouble

    val inputRDD2 = userItemRatingRDD.rdd.map(r => (r.getString(0), (r.getString(1), r.getDouble(2)))).groupByKey().flatMap { case (user, goods) =>
        val size = goods.size
      goods.toArray.sortWith(_._2 > _._2).take((ration * size).toInt).map(s => (user, s._1, s._2))
    }

    // 用户对商品的平均评分
    val score = spark.sql(
      """
        |SELECT  adr.storename, c.product_id, avg(c.score)
        |FROM sourcedata.s13_bl_comment c
        |JOIN sourcedata.s03_oms_order_sub o ON o.order_no = c.ordernumber
        |JOIN address_coordinate_store adr ON adr.address = regexp_replace(o.recept_address_detail, ' ', '')
        |GROUP BY adr.storename, c.product_id
        |
      """.stripMargin)

    val inputRDD = score.rdd.map { r => (r.getString(0), r.getString(1), r.getDouble(2))}.groupBy(_._1).flatMap { case (user, goods) =>
        val size = goods.size
        goods.toArray.sortWith(_._3 > _._3).take((0.5 * size).toInt)
    }
    inputRDD.cache

    // 给商店、商品编排序编号，保存在hdfs中
    val shopIndexRDD = inputRDD.map(_._1).distinct().zipWithIndex()
    val goodsIndexRDD = inputRDD.map(_._2).distinct().zipWithIndex()
    val shopIndexFile = rootDir + "/shop_index"
    val goodsIndexFile = rootDir + "/goods_index"
    val conf = new Configuration
    val fs = FileSystem.get(conf)
    if (fs.exists(new Path(shopIndexFile))) {
      fs.delete(new Path(shopIndexFile), true)
    }
    if (fs.exists(new Path(goodsIndexFile))) {
      fs.delete(new Path(goodsIndexFile), true)
    }
    shopIndexRDD.map(s => s._2 + "," + s._1).coalesce(1).saveAsTextFile(shopIndexFile)
    goodsIndexRDD.map(s => s._2 + "," + s._1).coalesce(1).saveAsTextFile(goodsIndexFile)

    // 将训练数据中 商店 code 和 商品 code 用他们的序号替换
    val trainFilePath = rootDir + "/shop_goods_rating"
    if (fs.exists(new Path(trainFilePath))) {
      fs.delete(new Path(trainFilePath), true)
    }
    val trainRDD = inputRDD.map(s => (s._1, (s._2, s._3))).join(shopIndexRDD).map { case (shopCode, ((goods_code, rating), shopIndex)) => (goods_code, (shopIndex, rating))}.join(goodsIndexRDD).
      map { case (goods_code, ((shopIndex, rating), goodsIndex)) => (shopIndex, goodsIndex, rating)}
    // 保存在 hdfs
    trainRDD.map(s => s"${s._1},${s._2},${s._3}").coalesce(1).saveAsTextFile(trainFilePath)

    inputRDD2.cache()
    // 基于用户的协同过滤
    val userBasedResultFile = rootDir + "/user_based_recommend"
    if (fs.exists(new Path(userBasedResultFile))) {
      fs.delete(new Path(userBasedResultFile), true)
    }

    val userBasedRecommendResult = UserBasedRecommend.userBaseRecommendRevised[String, String](inputRDD2)
    userBasedRecommendResult.map(s => (s._1, (s._2, s._3))).
      coalesce(2).saveAsTextFile(userBasedResultFile)

    // ****************************************************
    // 在 shell 里调用 mahout 基于物品的算法给用户推荐商品
    // ****************************************************
    val mahoutResult = rootDir + "/mahout_item_based"
    val mahoutTmpDir = rootDir + "/mahout_tmp"
    val mahoutItemBaseRecommendationCmd =
      s"""
        |/opt/spark/apache-mahout-distribution-0.12.2/bin/mahout  recommenditembased
        |--input $trainFilePath
        |--output $mahoutResult
        |--numRecommendations 2000
        |--maxPrefsPerUser 1000
        |--maxSimilaritiesPerItem 1000
        |--similarityClassname SIMILARITY_LOGLIKELIHOOD
        |--tempDir $mahoutTmpDir
      """.stripMargin

    ShellUtil.runShell(mahoutItemBaseRecommendationCmd)

  }

}
