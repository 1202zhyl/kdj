package com.bl.bd.algorithm

import com.bl.bd.util.{Configuration, SparkFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._


/**
  * Created by MK33 on 2016/12/8.
  */
object UserBasedRecommend {

  def main(args: Array[String]) {

    val result = train("/tmp/kdj/shop_goods_rating_user_based")
    val fs = Configuration.getHadoopFS
    val resultPath = "/tmp/kdj/user_based_result"
    if (fs.exists(new Path(resultPath))) {
      fs.delete(new Path(resultPath), true)
    }
    result.coalesce(2).saveAsTextFile(resultPath)

  }

  def train(file: String): RDD[(String, String, Double)] = {
    val spark = SparkFactory.getSparkContext
    val rawRDD = spark.textFile(file).map { line =>
      val words = line.split("\t")
      (words(0), (words(1), words(2).toDouble))
    }

    val trainRDD = rawRDD.groupByKey().flatMapValues(s => {
      val size = s.size
      s.toArray.sortWith(_._2 > _._2).take((0.5 * size).toInt).map(s => (s._1, s._2))
    }).map(s => (s._1, s._2._1, s._2._2))
    trainRDD.cache()
    userBaseRecommend(trainRDD)

  }

  /** 用于快到家 */
  def userBaseRecommend[k <% Ordered[k], v <% Ordered[v]](inputRDD: RDD[(String, String, Double)]): RDD[(String, String, Double)] =  {
    // (user, item, rating) => (item, (user, rating, userPrefNum))
    val item2User = inputRDD.groupBy(_._1).flatMap { s =>
      val size = s._2.size
      s._2.map(s => (s._2, (s._1, s._3, size)))
    }
    // (user, item, rating) => (item, (user, rating))
    val item2UserPair = item2User.join(item2User).map { case (item, ((user1, rating1, numPref1), (user2, rating2, numPref2))) =>
      ((user1, user2), (rating1, rating2, numPref1, numPref2, item))
    }.filter(s => s._1._2 < s._1._1)

    // 计算用户的相似度
    val userSimilarity = item2UserPair.groupByKey().flatMap { case ((u1, u2), goods) =>
      val a = goods.map(s => s._1 * s._2).sum
      Array((u1,(u2, a / math.pow(goods.head._3 * goods.head._4, 0.5))), (u2,(u1, a / math.pow(goods.head._3 * goods.head._4, 0.5))))
    }

    // 和用户最相似的 N 个用户
    val userMostSimilarity = userSimilarity.groupByKey().flatMap(s => s._2.toArray.sortWith(_._2 > _._2).take(100).map(s0 => (s._1, s0)))
    // 给用户推荐和他相似的用户喜欢的商品
    val user2Goods = userMostSimilarity.join(inputRDD.map(s => (s._1, (s._2, s._3)))).map { case (u1, ((u2, u1u2Sim), (item, u1Rating))) =>
      ((u2, item), u1u2Sim * u1Rating)
    }
    // 减去已经在用户感兴趣列表中的物品
    val userLikeGoodsFilter = user2Goods.subtractByKey(inputRDD.map(s => ((s._1, s._2), s._3))).map(s => (s._1._1, s._1._2, s._2))
    userLikeGoodsFilter

  }


  def train(trainRDD: RDD[(String, String, Double)]): Unit = {

  }

}
