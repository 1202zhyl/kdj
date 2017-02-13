package com.bl.bd

import com.bl.bd.util.SparkFactory

/**
 * Hello world!
 *
 */
object App{

  def main(args: Array[String]){

    val sc = SparkFactory.getSparkContext

    val r = sc.textFile("/tmp/kdj/item_result").flatMap(line => {
      val s = line.split("\t")
      s(1).split(",").map ( s2 => {
        val aa = s2.split(":")

        (s(0),aa(0), aa(1), aa(2))
      })
    })
    r.filter(_._1 == "华联超市浙中店").map(s => s._1 + "\t"  + s._2 + "\t" + s._3 + "\t" + s._4).coalesce(1).saveAsTextFile("/tmp/kdj/华联超市浙中店/item-based")

    sc.textFile("/tmp/kdj/华联超市浙中店/item-based").count()

    sc.textFile("/tmp/kdj/item_result").flatMap(line => line.split("\t")(1).split(",").map(s => (s, s.split(":").length))).
      filter(_._2 == 4).collect().foreach(println)

    def diff3(i: String, j: String): Unit = {
      val a = r.filter(_._1 == i)
      val b = r.filter(_._1 == j)
      println(a.count())
      println(b.count())
      println(a.map(_._2).subtract(b.map(_._2)))
    }
    diff3("联华超市福州店","联华超市福州店")
    r.filter(_._1 == "华联超市浙中店").map(_._2).subtract(r.filter(_._1 == "华联超市龙华西店").map(_._2)).count()


  }
}
