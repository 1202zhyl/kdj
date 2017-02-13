package com.bl.bd.kdj

import com.bl.bd.util.SparkFactory

/**
  * Created by MK33 on 2016/12/5.
  */
//object ProcessItemBasedResult {
//
//  def main(args: Array[String]) {
//
//    val sc = SparkFactory.getSparkContext
//    val spark = SparkFactory.getSparkSession()
//    import spark.implicits._
//
//    // 处理 mahout 计算结果
//    val goods = sc.textFile("/tmp/kdj/mahout_recommend").flatMap{ s => { val a = s.split("\t"); a(1).substring(1, a(1).length - 1).split(",").
//      map(s0 => { val a0 = s0.split(":"); (a(0), (a0(0), a0(1))) }) }}
//    val shopIndex = sc.textFile("/tmp/kdj/shop_index").map { s => { val a = s.split(","); (a(0), a(1)) } }
//    val goodsIndex = sc.textFile("/tmp/kdj/goods_index").map { s => { val a = s.split(","); (a(0), a(1)) } }
//
//    val finalResult = goods.join(shopIndex).map { case (shop, ((g, rating), shopName)) => (g, shopName) }.
//      join(goodsIndex).map { case (g, (shopName, goodsCode)) => (shopName, goodsCode) }
//    finalResult.toDF().createOrReplaceTempView("shop_goods")
//    spark.sql("select distinct t._1, g.sid, g.goods_sales_name  from shop_goods t join recommendation.goods_avaialbe_for_sale_channel g on g.sid = t._2 ").rdd.
//      map(r => (r.getString(0), r.getString(1))).groupByKey().mapValues(_.mkString(",")).map(s => s._1 + ":" + s._2).coalesce(1).saveAsTextFile("/tmp/kdj/item_result")
//
//  }
//
//}
