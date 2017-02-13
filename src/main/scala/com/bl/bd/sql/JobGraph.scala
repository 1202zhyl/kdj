package com.bl.bd.sql

import scala.collection.mutable

/**
  * Created by MK33 on 2016/12/27.
  */
object JobGraph {

  def main(args: Array[String]) {
    val sql1 =
      """
        |SELECT adr.storename, sum(oo1.sale_money_sum) AS s
        |FROM sourcedata.s03_oms_order oo1
        |JOIN sourcedata.s03_oms_order_sub oa ON oa.order_no = oo1.order_no
        |JOIN address_coordinate_store adr ON adr.address = regexp_replace(oa.recept_address_detail, ' ', '')
        |WHERE oo1.order_type_code = '25'
        |GROUP BY adr.storename
        |ORDER BY s DESC
        |
      """.stripMargin

//    val sql2 =
//      """
//        |DROP TABLE recommendation.user_preference_category_visit
//      """.stripMargin

    val sql3 = "CREATE TABLE recommendation.user_preference_category_visit as  " +
      " SELECT category_sid,date2,max(count) pv FROM " +
      " ( select count(*) count,substr(event_date,0,10) date2,category_sid  from recommendation.user_behavior_raw_data  " +
      " WHERE member_id IS NOT NULL AND member_id <> 'null' and   " +
      "dt >'$datebeg'  group by category_sid,substr(event_date,0,10),member_id having count<50) ucdata GROUP BY date2,category_sid"

    generatorGraph(Array(sql1, sql3))


  }


  def generatorGraph(SQLs: Array[String]): Unit = {
    val sqls = SQLs.distinct
    val nodeSet = new mutable.HashSet[Node]()
    for (sql <- sqls) {
      val a = SqlHelper.getInputOutputTable(sql)
      nodeSet add new Node(sql, a._1.map(_._1).toSet, if (a._2.nonEmpty) a._2.head._1 else null)
    }

    // 解析依赖关系
    for (node <- nodeSet) {
      val dependencyTable = node.dependency
      for (table <- dependencyTable) {
        nodeSet.filter(_.output == table).map { n =>
          if (n.to == null) n.to = mutable.HashSet(n) else n.to.add(node)
          if (node.from == null) node.from = mutable.HashSet[Node](n) else node.from.add(n)
        }
      }
    }

    println(nodeSet.size)

  }

}
