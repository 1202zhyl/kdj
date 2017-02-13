package com.bl.bd.etl

import com.bl.bd.util.SparkFactory

/**
  * Created by MK33 on 2016/12/9.
  */
object ItemBasedShopGoodsRating {

  def main(args: Array[String]) {
    run()
  }
  def run(): Unit = {
    val spark = SparkFactory.getSparkSession()

    val rootDir = "/tmp/kdj"
    // 读取订单数据：goods_code, storename, total_price, total_amt
    val orderRawRDD = spark.sql(
      """
        |select T.goods_code, T.storename, sum(T.sale_price) money, sum(T.sale_sum) amt
        |from (
        |SELECT distinct od.order_no, od.goods_code, adr.storename, od.sale_price, od.sale_sum
        |FROM sourcedata.s03_oms_order_detail od
        |JOIN sourcedata.s03_oms_order_sub oa ON oa.order_no = od.order_no
        |JOIN address_coordinate_store adr ON adr.address = regexp_replace(oa.recept_address_detail, ' ', '')
        |) T
        |group by T.storename, T.goods_code
        |
      """.stripMargin).distinct()
    orderRawRDD.cache()
    orderRawRDD.createOrReplaceTempView("order")

    val shopGoodsMax = spark.sql(
      """
        |select storename, max(amt) max_amt, min(amt) min_amt, max(money) max_money, min(money) min_money
        |from order
        |group by storename
        |
      """.stripMargin)

    shopGoodsMax.createOrReplaceTempView("store_max")

    // 算出每个商店每个商品的得分
    val storeGoods = spark.sql(
      """
        |select O.storename, O.goods_code,
        | if(M.min_money == M.max_money, 0.0, 2.5 * (O.money - M.min_money) / (M.max_money - M.min_money)) +
        | if(M.min_amt == M.max_amt, 0.0, 2.5 * (O.amt - M.min_amt) / (M.max_amt - M.min_amt))
        |from order O
        |join store_max M on M.storename = O.storename
        |
      """.stripMargin)

    storeGoods.rdd.map { row =>
      row.getString(0) + "\t" + row.getString(1) + "\t" + row.getDouble(2)
    }.coalesce(2).saveAsTextFile(rootDir + "/shop_goods_rating_item_based")





  }



}
