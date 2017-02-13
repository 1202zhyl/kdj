package com.bl.bd.algorithm

import com.bl.bd.util.{Configuration, ShellUtil, SparkFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD


/**
  * 这个包调用 mahout 计算基于物品的商品相似度的算法
  * Created by MK33 on 2016/12/8.
  */
object ItemBaseRecommend {

  def main(args: Array[String]) {

    val mahoutCmd =
      s"""
         |/opt/spark/apache-mahout-distribution-0.12.2/bin/mahout  recommenditembased
         |--input /tmp/kdj/shop_goods_rating_item_based_mahout
         |--output /tmp/kdj/result_item_based_mahout
         |--numRecommendations 2000
         |--maxPrefsPerUser 1000
         |--maxSimilaritiesPerItem 1000
         |--similarityClassname SIMILARITY_LOGLIKELIHOOD
         |--tempDir /tmp/kdj/mahout-tmp
         |
      """.stripMargin

    train("/tmp/kdj/shop_goods_rating_item_based", mahoutCmd, "shop_goods_similarity")

  }

  /** 调用 mahout 接口 */
  def train(file: String, cmd: String, hbaseTable: String): Unit = {

    val sc = SparkFactory.getSparkContext
    val rawRDD = sc.textFile(file)
    val trainRDD = rawRDD.map { line =>
      val words = line.split("\t")
      (words(0), (words(1), words(2).toDouble))
    }.groupByKey().flatMap { case (user, goods) =>
      val size = goods.size
      goods.toArray.sortWith(_._2 > _._2).take((size * 0.5).toInt).map(s => (user, s._1, s._2))
    }

    val stream = cmd.replace("\n", " ").split(" ").map(_.trim).filter(_.length > 0)
    var input: String = null
    var tempDir: String = null
    var output: String = null

    for ( i <-  0 until stream.length ) {
      stream(i) match {
        case "--input" => input = stream(i + 1)
        case "--tempDir" => tempDir = stream(i + 1)
        case "--output" => output = stream(i + 1)
        case _ =>
      }
    }
    require(input != null, "input path cannot be null ")
    require(tempDir != null, "tmp path cannot be null ")
    val fs = Configuration.getHadoopFS

    if (fs.exists(new Path(input))) {
      fs.delete(new Path(input), true)
    }
    if (fs.exists(new Path(tempDir))) {
      fs.delete(new Path(tempDir), true)
    }

    // 给商店、商品编排序编号，保存在hdfs中
    val shopIndexRDD = trainRDD.map(_._1).distinct().zipWithIndex()
    val goodsIndexRDD = trainRDD.map(_._2).distinct().zipWithIndex()
    val shopIndexFile = tempDir + "/shop_index"
    val goodsIndexFile = tempDir + "/goods_index"
    if (fs.exists(new Path(shopIndexFile))) {
      fs.delete(new Path(shopIndexFile), true)
    }
    if (fs.exists(new Path(goodsIndexFile))) {
      fs.delete(new Path(goodsIndexFile), true)
    }
    shopIndexRDD.map(s => s._2 + "," + s._1).coalesce(1).saveAsTextFile(shopIndexFile)
    goodsIndexRDD.map(s => s._2 + "," + s._1).coalesce(1).saveAsTextFile(goodsIndexFile)

    val shopIndexRead = sc.textFile(shopIndexFile).map { line => { val s = line.split(","); (s(1), s(0).toInt) }}
    val goodsIndexRead = sc.textFile(goodsIndexFile).map { line => { val s = line.split(","); (s(1), s(0).toInt) }}

    // 将训练数据中 商店 code 和 商品 code 用他们的序号替换
    val trainRDD2 = trainRDD.map(s => (s._1, (s._2, s._3))).join(shopIndexRead).map { case (shopCode, ((goods_code, rating), shopIndex)) => (goods_code, (shopIndex, rating))}.
      join(goodsIndexRead).
      map { case (goods_code, ((shopIndex, rating), goodsIndex)) => (shopIndex, goodsIndex, rating)}
    // 保存在 hdfs
    trainRDD2.map(s => s"${s._1},${s._2},${s._3}").coalesce(1).saveAsTextFile(input)

    // 开始调用 mahout 计算，将输出文件夹清空, 这一步有风险，如果 mahout调用失败，则上一次的结果也被删除了
    if (fs.exists(new Path(output))) {
      fs.delete(new Path(output), true)
    }
    val exit = mahout(cmd)
    if (exit != 0) {
      println("mahout error!")
      sys.exit(1)
    }

    // 处理 mahout 计算结果，输出 shop_name, goods_code, goods_name, rating
    val goods = sc.textFile(output).flatMap { s =>
      val a = s.split("\t")
      a(1).substring(1, a(1).length - 1).split(",").map { s0 =>
        val a0 = s0.split(":")
        (a(0), (a0(0), a0(1)))
      }
    }

    val shopIndex = sc.textFile(shopIndexFile).map { s => { val a = s.split(","); (a(0), a(1)) } }
    val goodsIndex = sc.textFile(goodsIndexFile).map { s => { val a = s.split(","); (a(0), a(1)) } }

    val spark = SparkFactory.getSparkSession()
    import spark.implicits._
    val finalResult = goods.join(shopIndex).map { case (shop, ((goods, rating), shopName)) => (goods, (shopName, rating)) }.
      join(goodsIndex).map { case (g, ((shopName, rating), goodsCode)) => RecommendResult(shopName, goodsCode, rating) }

    finalResult.toDF().createOrReplaceTempView("shop_goods")

    spark.sql(
      """
        |select distinct t.shop_name, g.sid, g.goods_sales_name, t.rating
        |from shop_goods t
        |join recommendation.goods_avaialbe_for_sale_channel g on g.sid = t.goods_code
        |
      """.stripMargin).
      rdd.map(r => (r.getString(0), (r.getString(1), r.getString(2), r.getString(3)))).
      groupByKey().mapValues(_.map(s => s._1 + ":" + s._2.replaceAll(":|,", "") + ":" + s._3).mkString(",")).map(s => s._1 + "\t" + s._2).
      coalesce(1).saveAsTextFile("/tmp/kdj2/item_result_final")

    // 保存在 HBase 中
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





  }

  def mahout(cmd: String): Int = {
    // ****************************************************
    // 在 shell 里调用 mahout 基于物品的算法给用户推荐商品
    // ****************************************************
    ShellUtil.runShell(cmd)
  }

  /** (user, item, pref) */
  def train(trainRDD: RDD[(String, String, Double)]): Unit = {


  }



}

case class RecommendResult(shop_name: String, goods_code: String, rating: String)
