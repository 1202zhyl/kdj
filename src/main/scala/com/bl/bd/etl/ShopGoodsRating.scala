package com.bl.bd.etl

import com.bl.bd.util.{Configuration, SparkFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._

/**
  *
  * Created by MK33 on 2016/12/8.
  */
object ShopGoodsRating {

  def main(args: Array[String]): Unit = {
    run()
  }

  def run(): Unit = {

    val spark = SparkFactory.getSparkSession()
    val rootDir = "/tmp/kdj"
    import spark.implicits._
    val orderRawRDD = spark.sql(
      """
        |SELECT od.order_no, od.goods_code, adr.storename, od.sale_price, od.sale_sum
        |FROM sourcedata.s03_oms_order_detail od
        |JOIN sourcedata.s03_oms_order_sub oa ON oa.order_no = od.order_no
        |JOIN address_coordinate_store adr ON adr.address = regexp_replace(oa.recept_address_detail, ' ', '')
        |
      """.stripMargin).distinct()
    val a = orderRawRDD.select("goods_code", "storename", "sale_price", "sale_sum").
      groupBy("goods_code", "storename").agg(sum($"sale_sum" * $"sale_price") as "money", sum("sale_sum") as "amt")

    a.createOrReplaceTempView("spark_a")

    // 每个门店单品销售额的最大值和销售的最大数量
    val b = a.select("money", "amt", "storename").groupBy("storename").
      agg(max("money") as "max_money", min("money") as "min_money", max("amt") as "max_amt", min("amt") as "min_amt")

    b.createOrReplaceTempView("spark_b")

    spark.udf.register("my_normalization", (min: Double, max: Double, v: Double) => { if (min == max) 0.0 else 2.5 * (v - min) / (max - min) } )

    val c = spark.sql(
      """
        |select a.storename, a.goods_code, my_normalization(b.min_money, b.max_money, a.money) money_score,
        |my_normalization(b.min_amt, b.max_amt, a.amt) amt_score
        |from spark_a a
        |join spark_b b on b.storename = a.storename
        |
      """.stripMargin)

    c.createOrReplaceTempView("result")

    val userItemRatingRDD = spark.sql(
      """
        |select storename, goods_code, money_score + amt_score as rating
        |from result
        |order by rating desc
        |
      """.stripMargin)


    // 基于用户的推荐算法数据集
    val userBasedRecommendTrainPath = rootDir + "/shop_goods_rating_user_based"
    val fs = Configuration.getHadoopFS
    if (fs.exists(new Path(userBasedRecommendTrainPath))) {
      fs.delete(new Path(userBasedRecommendTrainPath), true)
    }

    userItemRatingRDD.rdd.map(r => r.getString(0) + "\t" + r.getString(1) + "\t" + r.getDouble(2)).coalesce(2).
      saveAsTextFile(userBasedRecommendTrainPath)

    val score = spark.sql(
      """
        |SELECT  adr.storename, c.product_id, avg(c.score) as score
        |FROM sourcedata.s13_bl_comment c
        |JOIN sourcedata.s03_oms_order_sub o ON o.order_no = c.ordernumber
        |JOIN address_coordinate_store adr ON adr.address = regexp_replace(o.recept_address_detail, ' ', '')
        |GROUP BY adr.storename, c.product_id
        |order by score desc
        |
      """.stripMargin)

    // 基于物品的推荐算法数据集
    val itemBasedRecommendTrainPath = rootDir + "/shop_goods_rating_item_based"
    if (fs.exists(new Path(itemBasedRecommendTrainPath))) {
      fs.delete(new Path(itemBasedRecommendTrainPath), true)
    }

    score.rdd.map { r => r.getString(0) + "\t" + r.getString(1) + "\t" + r.getDouble(2) }.
      coalesce(2).saveAsTextFile(itemBasedRecommendTrainPath)


  }

}
