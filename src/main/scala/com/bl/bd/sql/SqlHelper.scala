package com.bl.bd.sql


import java.util

import org.apache.hadoop.hive.ql.parse._

import scala.collection.JavaConversions._
import scala.collection.immutable.{SortedSet, TreeMap}

/**
  *
  * Created by MK33 on 2016/12/26.
  */

object Oper extends Enumeration {
  type Oper = Value

  val SELECT = Value("select")
  val INSERT = Value("insert")
  val DROP = Value("drop")
  val TRUNCATE = Value("truncate")
  val LOAD = Value("load")
  val CREATE_TABLE = Value("create_table")
  val ALTER = Value("alter")
  val ALTER_TABLE = Value("alter_table_rename")

}

import com.bl.bd.sql.Oper._

object SqlHelper {


  var joinClause = false
  val dbMap = new util.HashMap[String/*table*/, java.util.List[String/*column*/]]()
  var nowQueryTable: String = null
  val tables = new util.HashSet[(String, Oper)]()
  val tableNameStack = new util.Stack[String]
  val aliasTable = new util.HashSet[String]()
  val operStack = new util.Stack[Oper]
  var oper: Oper = null

  def prepareWork = {
    joinClause = false
    nowQueryTable = null
    tables.clear()
    tableNameStack.clear()
    operStack.clear()
    oper = null

  }
  /** 根据输入 sql， 得到输入、输出表 */
  def getInputOutputTable(sql: String): (List[(String, Oper)], List[(String, Oper)]) = {
    require(sql != null && sql.length > 0, "sql cannot be null nor empty")
    prepareWork
    val parserDriver = new ParseDriver()
    val astNode = parserDriver.parse(sql)
    Console println astNode.dump()
    parseIterator(astNode)
    tables.foreach(s =>println(s._1 + " \t" + s._2))
    val tmp = tables.partition(s => s._2 == Oper.SELECT || s._2 == Oper.ALTER || s._2 == Oper.DROP)

    (tmp._1.toArray.sortBy(_._1).toList, tmp._2.toArray.sortBy(_._1).toList)

  }
    def parseAST(aSTNode: ASTNode): Unit = {
      parseIterator(aSTNode)
    }

    def parseIterator(aSTNode: ASTNode): util.Set[String] = {
      val set = new util.HashSet[String]()
      prepareToParseCurrentNodeAndChilds(aSTNode)
      set.addAll(parseChildNode(aSTNode))
      set.addAll(parseCurrentNode(aSTNode, set))
      endParseCurrentNode(aSTNode)
      set
    }

    def prepareToParseCurrentNodeAndChilds(node: ASTNode): Unit = {

      if (node.getToken != null) {
        node.getToken.getType match {
          case HiveParser.TOK_RIGHTOUTERJOIN =>
            println("TOK_RIGHTOUTERJOIN")
            joinClause = true
          case HiveParser.TOK_LEFTOUTERJOIN =>
            println("TOK_LEFTOUTERJOIN")
            joinClause = true
          case HiveParser.TOK_JOIN =>
            println("TOK_JOIN")
            joinClause = true
          case HiveParser.TOK_QUERY =>
            println("TOK_QUERY")
            tableNameStack.push(nowQueryTable)
            operStack.push(oper)
            nowQueryTable = ""
            oper = Oper.SELECT
          case HiveParser.TOK_INSERT =>
            tableNameStack.push(nowQueryTable)
            operStack.push(oper)
            oper = Oper.INSERT
          case HiveParser.TOK_SELECT =>
            tableNameStack.push(nowQueryTable)
            operStack.push(oper)
            oper = Oper.SELECT
          case HiveParser.TOK_DROPTABLE =>
            println("drop")
            tableNameStack.push(nowQueryTable)
            operStack.push(oper)
            oper = Oper.DROP
          case HiveParser.TOK_TRUNCATETABLE =>
            println("truncate")
            tableNameStack.push(nowQueryTable)
            operStack.push(oper)
            oper = Oper.TRUNCATE
          case HiveParser.TOK_LOAD =>
            oper = Oper.LOAD
          case HiveParser.TOK_CREATETABLE =>
            println("TOK_CREATETABLE")
            tableNameStack.push(nowQueryTable)
            operStack.push(oper)
            oper = Oper.CREATE_TABLE
          case HiveParser.TOK_ALTERTABLE =>
            println("TOK_ALTERTABLE")
            tableNameStack.push(nowQueryTable)
            operStack.push(oper)
            oper = Oper.ALTER
          case HiveParser.TOK_ALTERTABLE_RENAME =>
            println("TOK_ALTERTABLE_RENAME")
            tableNameStack.push(nowQueryTable)
            operStack.push(oper)
            oper = Oper.ALTER_TABLE
          case e =>
        }

//        if (node.getToken != null
//          && node.getToken.getType >= HiveParser.TOK_ALTERDATABASE_PROPERTIES
//          && node.getToken.getType <= HiveParser.TOK_ALTERVIEW_RENAME) {
//          oper = Oper.ALTER
//        }

    }

  }

    def parseChildNode(ast: ASTNode): util.Set[String] = {
      val hashSet = new util.HashSet[String]()
      val childCount = ast.getChildCount
      if (childCount > 0) {
        for (i <- 0 until ast.getChildren.size()) {
          val node = ast.getChild(i).asInstanceOf[ASTNode]
          hashSet.addAll(parseIterator(node))
        }
      }
      hashSet
    }

  def parseCurrentNode(ast: ASTNode, set: util.Set[String]): util.Set[String] = {
    if (ast.getToken != null) {
      ast.getToken.getType match {
        case HiveParser.TOK_TABLE_PARTITION =>
          println("TOK_TABLE_PARTITION " + ast.getChildCount)
          //case HiveParser.TOK_TABNAME:
          if (ast.getChildCount == 2) {
            val table = BaseSemanticAnalyzer.getUnescapedName(ast.getChild(0).asInstanceOf[ASTNode])
            nowQueryTable = table
            tables.add((table, oper))
          }

        case HiveParser.TOK_TAB =>
          val tableTab = BaseSemanticAnalyzer.getUnescapedName(ast.getChild(0).asInstanceOf[ASTNode])
          if (oper == Oper.SELECT) {
            nowQueryTable = tableTab
          }
          tables.add((tableTab, oper))
        case HiveParser.TOK_TABREF =>
          val tabTree = ast.getChild(0).asInstanceOf[ASTNode]
          val tableName: String =
            if ((tabTree.getChildCount == 1))
              BaseSemanticAnalyzer.getUnescapedName(tabTree.getChild(0).asInstanceOf[ASTNode])
          else
            BaseSemanticAnalyzer.getUnescapedName(tabTree.getChild(0).asInstanceOf[ASTNode]) + "." + tabTree.getChild(1)
          if (oper == Oper.SELECT) {
            if (joinClause && "" != nowQueryTable) {
              nowQueryTable += "&" + tableName
            }
            else {
              nowQueryTable = tableName
            }
            // table alias name
            if (ast.getChild(1) != null) {
              aliasTable.add(ast.getChild(1).getText)
            }
            set.add(tableName)
          }
          tables.add((tableName, oper))
        case HiveParser.TOK_TABNAME =>
          nowQueryTable =
            if ((ast.getChildCount == 1))
              BaseSemanticAnalyzer.getUnescapedName(ast.getChild(0).asInstanceOf[ASTNode])
            else
              BaseSemanticAnalyzer.getUnescapedName(ast.getChild(0).asInstanceOf[ASTNode]) + "." + ast.getChild(1)
          if (!aliasTable.contains(nowQueryTable)) tables.add((nowQueryTable, oper))

        case HiveParser.TOK_SUBQUERY =>
          if (ast.getChildCount == 2) { // 子查询对应的表名
            aliasTable.add(ast.getChild(1).getText)
          }

        case HiveParser.TOK_CREATETABLE =>
          val createTableTree = ast.getChild(0).asInstanceOf[ASTNode]
          if (createTableTree.getChildCount == 2) {

          } else {

          }
        case HiveParser.TOK_ALTERTABLE_ADDPARTS =>
        case HiveParser.TOK_ALTERTABLE_RENAME =>
        case HiveParser.TOK_ALTERTABLE_ADDCOLS =>
          val alterTableName = ast.getChild(0).asInstanceOf[ASTNode]
          tables.add((alterTableName.getText, oper))
        case e =>
      }
    }
    set
  }

  private def endParseCurrentNode(ast: ASTNode) {
    if (ast.getToken != null) {
      ast.getToken.getType match {
        case HiveParser.TOK_RIGHTOUTERJOIN |
             HiveParser.TOK_LEFTOUTERJOIN |
             HiveParser.TOK_JOIN =>
          joinClause = false
        case HiveParser.TOK_QUERY |
             HiveParser.TOK_INSERT |
             HiveParser.TOK_SELECT =>
          nowQueryTable = tableNameStack.pop
          oper = operStack.pop
        case e =>
      }
    }
  }

  def unescapeIdentifier(str: String): String = {
    if (str == null) null
    else if (str.startsWith("`") && str.endsWith("`")) str.substring(1, str.length - 1)
    else str

  }




  /** just for test */
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

    val sql2 =
      """
        |DROP TABLE recommendation.user_preference_category_visit
      """.stripMargin

    val sql3 = "create table recommendation.user_preference_category_visit as  " +
      " SELECT category_sid,date2,max(count) pv FROM " +
      " ( select count(*) count,substr(event_date,0,10) date2,category_sid  from recommendation.user_behavior_raw_data  " +
      " WHERE member_id IS NOT NULL AND member_id <> 'null' and   " +
      "dt >'$datebeg'  group by category_sid,substr(event_date,0,10),member_id having count<50) ucdata GROUP BY date2,category_sid"


    val sql4: String = "INSERT overwrite TABLE recommendation.user_behavior_raw_data_cm PARTITION (dt='$yestoday') " +
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
      "AND ubrd.event_date=regdata.rd_event_date"


    val sql5: String = "INSERT INTO TABLE recommendation.user_behavior_raw_data partition(dt='$yestoday')\n " +
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
      "   ON apse.cookie_id = ckmem.cookie_id"

    val sql6 = "truncate table recommendation.user_behavior_raw_data  partition(dt='$yestoday')"
    getInputOutputTable(sql3)

  }

}
