package com.bl.bd.rating

import com.bl.bd.recommend.UserBasedRecommend
import com.bl.bd.util._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.functions._

/**
  * 计算门店周围商品的打分，算法：
  * 求出每个门店周围商品销量和销售额的最大值，然后进行归一化处理，最大分值为 5 分。
  * 然后将商品的销量得分和销售额得分加在一起，得到商品的最终得分。
  * Created by MK33 on 2016/12/16.
  */
object PriceAndAmtRating {

  // 给每个门店周围商品打分
  def run(outputRootPath: String, hbaseTable: String): Unit = {

    val spark = SparkFactory.getSparkSession()
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

    spark.udf.register("my_normalization", (min: Double, max: Double, v: Double) => {
      if (min == max) -1.0 else 5.0 * (v - min) / (max - min)
    })

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


    val inputRDD2 = if (ration < 1.0) userItemRatingRDD.rdd.map(r => (r.getString(0), (r.getString(1), r.getDouble(2)))).groupByKey().flatMap { case (user, goods) =>
      val size = goods.size
      goods.toArray.sortWith(_._2 > _._2).take((ration * size).toInt).map(s => (user, s._1, s._2))
    } else userItemRatingRDD.rdd.map(r => (r.getString(0), r.getString(1), r.getDouble(2)))

    // 保存到 hdfs
    val outputPath = if (outputRootPath.endsWith("/")) outputRootPath + "shop_goods_rating" else outputRootPath + "/shop_goods_rating"
    val fs = Configuration.getHadoopFS
    if (fs.exists(new Path(outputPath))) {
      fs.delete(new Path(outputPath), true)
    }

    inputRDD2.map(s => s._1 + "\t" + s._2 + "\t" + s._3).saveAsTextFile(outputPath)

    userItemRatingRDD.createOrReplaceTempView("user_item_rating")

    // 保存到 HBase
    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTable)
//    conf.set("mapred.output.key.class", classOf[ImmutableBytesWritable].getName)
//    conf.set("mapred.output.value.class", classOf[ImmutableBytesWritable].getName)
//    conf.set("mapreduce.outputformat.class", classOf[TableOutputFormat[Put]].getName)
//    conf.set("hbase.zookeeper.quorum", "slave23.bl.bigdata,slave24.bl.bigdata,slave21.bl.bigdata,slave22.bl.bigdata,slave25.bl.bigdata")

    val job = Job.getInstance(conf)
    job.setOutputFormatClass(classOf[TableOutputFormat[Put]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Put])
    spark.sql(
      """
        |select u.storename, u.goods_code, u.rating, g.goods_sales_name
        |from user_item_rating u
        |join recommendation.goods_avaialbe_for_sale_channel g on g.sid = u.goods_code
        |
      """.stripMargin).rdd.map { row =>
      (row.getString(0), row.getString(1), row.getDouble(2), row.getString(3))
    }.map { case (storeName, goodsCode, rating, goodsSaleName) =>
      val put = new Put(Bytes.toBytes(storeName + "_" + goodsCode))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("goods_sale_name"), Bytes.toBytes(goodsSaleName))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rating"), Bytes.toBytes(rating))
      (new ImmutableBytesWritable(Bytes.toBytes(storeName)), put)
    }.saveAsNewAPIHadoopDataset(job.getConfiguration)


  }

}
