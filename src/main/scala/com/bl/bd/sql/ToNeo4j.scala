package com.bl.bd.sql


import org.neo4j.driver.v1._

/**
  * Created by MK33 on 2016/12/30.
  */
object ToNeo4j {

  var driver: Driver = _



  def main(args: Array[String]) {

    val sqls = Array(
      """
                        |SELECT adr.storename, sum(oo1.sale_money_sum) AS s
                        |FROM sourcedata.s03_oms_order oo1
                        |JOIN sourcedata.s03_oms_order_sub oa ON oa.order_no = oo1.order_no
                        |JOIN address_coordinate_store adr ON adr.address = regexp_replace(oa.recept_address_detail, ' ', '')
                        |WHERE oo1.order_type_code = '25'
                        |GROUP BY adr.storename
                        |ORDER BY s DESC
                        |
                      """.stripMargin,
      """
        |DROP TABLE recommendation.user_preference_category_visit
      """.stripMargin,
      "create table recommendation.user_preference_category_visit as  " +
        " SELECT category_sid,date2,max(count) pv FROM " +
        " ( select count(*) count,substr(event_date,0,10) date2,category_sid  from recommendation.user_behavior_raw_data  " +
        " WHERE member_id IS NOT NULL AND member_id <> 'null' and   " +
        "dt >'$datebeg'  group by category_sid,substr(event_date,0,10),member_id having count<50) ucdata GROUP BY date2,category_sid",
      "INSERT overwrite TABLE recommendation.user_behavior_raw_data_cm PARTITION (dt='$yestoday') " +
        "SELECT ubrd.cookie_id,\n" +
        "       regdata.registration_id,\n" +
        "       ubrd.session_id,\n" +
        "       ubrd.goods_sid,\n" +
        "       ubrd.goods_name,\n" +
        "       ubrd.quanlity,\n" +
        "       ubrd.event_date,\n" +
        "       ubrd.behavior_type,\n" +
        "       ubrd.channel,\n" +
        "       ubrd.category_sid\n" +
        "FROM (SELECT * FROM recommendation.user_behavior_raw_data WHERE dt='$yestoday') ubrd\n" +
        "LEFT JOIN\n" +
        "  (SELECT cookie_id,registration_id,rd_event_date FROM\n" +
        "(SELECT rd.cookie_id,\n" +
        "             dv.registration_id,\n" +
        "             rd.event_date rd_event_date,\n" +
        "             dv.event_date,row_number() OVER (partition by  rd.cookie_id,rd.event_date ORDER BY dv.event_date desc) AS rank\n" +
        "      FROM\n" +
        "        (SELECT cookie_id,\n" +
        "                event_date\n" +
        "         FROM recommendation.user_behavior_raw_data\n" +
        "         WHERE dt='$yestoday') rd\n" +
        "      LEFT JOIN\n" +
        "        (SELECT DISTINCT cookie_id,\n" +
        "                registration_id,\n" +
        "                event_date\n" +
        "         FROM sourcedata.s13_api_registered_data_visit WHERE registration_id <>'null') dv ON rd.cookie_id=dv.cookie_id\n" +
        "      WHERE rd.event_date>dv.event_date) cmdata WHERE cmdata.rank=1\n" +
        "      ) regdata ON ubrd.cookie_id=regdata.cookie_id\n" +
        "AND ubrd.event_date=regdata.rd_event_date",

      "INSERT INTO TABLE recommendation.user_behavior_raw_data partition(dt='$yestoday')\n " +
        "SELECT /*+STREAMTABLE(apse) */ apse.cookie_id,\n" +
        "                               ckmem.member_id,\n" +
        "                               apse.session_id,\n" +
        "                               apse.product_id,\n" +
        "                               apse.product_name,\n" +
        "                               1,\n" +
        "                               apse.event_date,\n" +
        "                               '1000',\n" +
        "                               substr(apse.page_id,0,instr(apse.page_id, '_') - 1),\n" +
        "                               dpc.category_id\n" +
        "FROM  (SELECT DISTINCT cookie_id,  session_id,    product_id,     product_name, event_date,         page_id   " +
        "  FROM sourcedata.s13_api_product_scan_event\n " +
        "   WHERE dt ='$yestoday' ) apse  INNER JOIN sourcedata.s06_pcm_mdm_goods goods ON goods.sid = apse.product_id\n   " +
        "INNER JOIN  (SELECT DISTINCT product_id,                 category_id   " +
        "   FROM idmdata.dim_management_category) dpc ON goods.pro_sid = dpc.product_id   LEFT JOIN     " +
        "   (SELECT DISTINCT cookie_id,page_id, rpt_attribute_10 member_id FROM sourcedata.s13_api_page_scan_event WHERE dt ='$yestoday') ckmem\n" +
        "   ON apse.cookie_id = ckmem.cookie_id",

      "truncate table recommendation.user_behavior_raw_data  partition(dt='$yestoday')"

    )
    val driver = getDriverManager



    // HiveTable: hive 中的表
    // HiveJob: hive sql job

    for (i <- 0 until sqls.length) {
      val sql = sqls(i)
      val (inputTable, outputTable) = SqlHelper.getInputOutputTable(sql)
      val session1 = getNeo4jSession
      createSession(session1, s""" CREATE (job:HiveJob {name: "$i", sql: "$sql" }) """)
      session1.close()
      inputTable.foreach { in =>
        val matcher = s"""match (ee:HiveTable {name: "${in._1}"}) return ee """
        val session = getNeo4jSession
        val result = getResult(session, matcher)
        if (!result.hasNext) {
          val create = s"""CREATE (table:HiveTable {name: "${in._1}"}) """
          val session = getNeo4jSession
          createSession(session, create)
          session.close()
        }

        val session2 = getNeo4jSession
        val create =
          s"""
             |MATCH (job:HiveJob {name: "$i"})
             |MATCH (table:HiveTable {name: "${in._1}"})
             |CREATE (job)-[:${in._2}]->(table)
             |""".stripMargin
        createSession(session2, create)
        session2.close()

      }

      outputTable.foreach { out =>
        val matcher = s"""MATCH (n:HiveTable {name: "${out._1}"}) return n """
        val session = getNeo4jSession
        val result = getResult(session, matcher)
        session.close()
        if (!result.hasNext) {
            val session = getNeo4jSession
            createSession(session, s"""CREATE (:HiveTable {name: "${out._1}"}) """)
          session.close()
        }
        val session2 = getNeo4jSession
        createSession(session2,
            s"""
               |MATCH (job:HiveJob {name: $i})
               |MATCH (table:HiveTable {name: "${out._1}" })
               |CREATE (job)-[:${out._2}]->(table)
               |""".stripMargin)
        session2.close()
        }
      }

    driver.close()



  }

  def getResult(session: Session, sql: String): StatementResult = {
    println("Match : \n" + sql)
    val tx = session.beginTransaction()
    val r = tx.run(sql)
    r
  }


  def createSession(session: Session, sql: String): Unit = {
    println("create : \n" + sql)
    val session = getNeo4jSession
    val tx = session.beginTransaction()
    tx.run(sql)
    tx.success()
    tx.close()
  }


  def getDriverManager: Driver = {
    if (driver == null) {
      synchronized {
        driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic("neo4j", "make"));
      }
    }
    driver
  }

  def getNeo4jSession: Session = {
    getDriverManager.session()
  }

}
