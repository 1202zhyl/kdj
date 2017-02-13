package com.bl.bd.algorithm

import com.bl.bd.util.{Configuration, ShellUtil, SparkFactory}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job

/**
  * Created by MK33 on 2016/12/16.
  */
object ItemSimilarityMahout {

  def main(args: Array[String]) {
    val cmd =
      """
      |/opt/spark/apache-mahout-distribution-0.12.2/bin/mahout itemsimilarity
      |--input /tmp/kdj2/shop_goods_rating_item_based_mahout
      |--output /tmp/kdj2/goods_similarity/item_based_mahout
      |--similarityClassname SIMILARITY_LOGLIKELIHOOD
      |--tempDir /tmp/kdj2/mahout-tmp
      |
    """.stripMargin

    val file = "/tmp/kdj2/shop_goods_rating"

    train(file, cmd, "shop_goods_similarity", "test")


  }


  def train(file: String, cmd: String, hbaseTable: String, hbaseKeyPrefix: String): Unit = {

    val sc = SparkFactory.getSparkContext
    val rawRDD = sc.textFile(file)
    val trainRDD = rawRDD.map { line =>
      val words = line.split("\t")
      (words(0), words(1), words(2).toDouble)
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

    val exit = ShellUtil.runShell(cmd)

    if (exit != 0) {
      println("mahout error!")
      sys.exit(1)
    }

    // 处理 mahout 计算结果，输出 goods_code1, goods_code2, rating
    val goods = sc.textFile(output).map { s =>
      val a = s.split("\t")
      (a(0), (a(1), a(2)))
    }

    val goodsIndex = sc.textFile(goodsIndexFile).map { s => { val a = s.split(","); (a(0), a(1)) } }

    val spark = SparkFactory.getSparkSession()
    import spark.implicits._
    val finalResult = goods.join(goodsIndex).map { case (goods1Index, ((goods2Index, similarity), goods1Code)) => (goods2Index, (goods1Code, similarity)) }.
      join(goodsIndex).map { case (goods2Index, ((goods1Code, similarity), goods2Code)) => RecommendResult2(goods1Code, goods2Code, similarity) }

    finalResult.toDF().createOrReplaceTempView("goods_similarity")

    val goodsSimilarityOutput = output + "_final"
    if (fs.exists(new Path(goodsSimilarityOutput))) {
      fs.delete(new Path(goodsSimilarityOutput), true)
    }

    spark.sql(
      """
        |select distinct t.goods1_code, g1.goods_sales_name, t.goods2_code, g2.goods_sales_name, t.similarity
        |from goods_similarity t
        |join recommendation.goods_avaialbe_for_sale_channel g1 on g1.sid = t.goods1_code
        |join recommendation.goods_avaialbe_for_sale_channel g2 on g2.sid = t.goods2_code
        |
      """.stripMargin).
      rdd.map(r => ((r.getString(0), r.getString(1)), (r.getString(2), r.getString(3), r.getString(4).toDouble))).
      groupByKey().mapValues(_.toArray.sortWith(_._3 > _._3).map(s => s._1 + ":" + s._2.replaceAll(":|,", "") + ":" + s._3).mkString(",")).
      map(s => s._1._1 + "," + s._1._2.replaceAll(":|,", "") + "\t" + s._2).
      coalesce(1).saveAsTextFile(goodsSimilarityOutput)

    // 保存在 HBase 中
    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTable)
    conf.set("hbase.zookeeper.quorum", "slave23.bl.bigdata,slave24.bl.bigdata,slave21.bl.bigdata,slave22.bl.bigdata,slave25.bl.bigdata")

    val job = Job.getInstance(conf)
    job.setOutputFormatClass(classOf[TableOutputFormat[Put]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Put])

    spark.sql(
      """
        |select distinct t.goods1_code, g1.goods_sales_name, t.goods2_code, g2.goods_sales_name, t.similarity
        |from goods_similarity t
        |join recommendation.goods_avaialbe_for_sale_channel g1 on g1.sid = t.goods1_code
        |join recommendation.goods_avaialbe_for_sale_channel g2 on g2.sid = t.goods2_code
        |
      """.stripMargin).
      rdd.map(r => ((r.getString(0), r.getString(1)), (r.getString(2), r.getString(3), r.getString(4).toDouble))).
      groupByKey().map { case ((goodsCode, goodsName), mostLikeGoods) =>
      val put = new Put(Bytes.toBytes(hbaseKeyPrefix + goodsCode))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("goods_sale_name"), Bytes.toBytes(goodsName.replaceAll(":|,", "")))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("most_like_goods"), Bytes.toBytes(mostLikeGoods.toArray.
        sortWith(_._3 > _._3).map(s => s._1 + ":" + s._2.replaceAll(":|,", "") + ":" + s._3).mkString(",")))
      (new ImmutableBytesWritable(Bytes.toBytes(goodsCode)), put)
    }.saveAsNewAPIHadoopDataset(job.getConfiguration)



  }


  def mahout(cmd: String): Int = {
    // ****************************************************
    // 在 shell 里调用 mahout 基于物品的算法给用户推荐商品
    // ****************************************************
    ShellUtil.runShell(cmd)
  }



}

case class RecommendResult2(goods1_code: String, goods2_code: String, similarity: String)