package com.bl.bd.sql

import org.junit.Test

/**
  * Created by MK33 on 2016/12/27.
  */
@Test
class SqlHelperTest {

  @Test
  def selectTest = {
    val selectSql =
      """
        |SELECT goodsnstock.*,CASE WHEN gstock.stock IS NULL THEN 0 ELSE gstock.stock END as stock
        |FROM
        |  (SELECT goods_sale.*,
        |          ppu.sid pic_sid,
        |          ppu.url
        |   FROM
        |     (SELECT goods.*,CASE WHEN chan_sale.sale_status IS NULL THEN 0 ELSE 4 END as sale_status
        |      FROM recommendation.goods_basic_info goods
        |      LEFT JOIN
        |        (SELECT goods_sid,
        |                channel_sid,sale_status
        |         FROM sourcedata.s06_pcm_chan_sale
        |         WHERE sale_status=4.0
        |           AND is_del=0.0) chan_sale ON chan_sale.goods_sid=goods.sid
        |      AND chan_sale.channel_sid = goods.channel_sid) goods_sale
        |    JOIN
        |     (SELECT goods_sid,
        |             pic.sid,
        |             url
        |      FROM sourcedata.s06_pcm_picture pic,
        |           sourcedata.s06_pcm_picture_url pic_url
        |      WHERE pic.is_primary=1.0
        |        AND pic.is_enabled=1.0
        |        AND pic.is_deleted =0.0
        |        AND pic_url.is_deleted=0.0
        |        AND pic_url.spec_sid='6'
        |        AND pic.sid = pic_url.pic_sid) ppu ON goods_sale.sid = ppu.goods_sid)goodsnstock
        |LEFT JOIN
        |  (SELECT goods_sid,1 AS stock
        |   FROM sourcedata.s06_pcm_stock
        |   WHERE sale_stock_sum - occupy_stock_sum >0
        |     AND is_deleted<1
        |     AND stock_type <1
        |     AND (active_code = 'null' or active_code is null)
        |     AND (shop_sid = 'null' or shop_sid is null)) gstock ON gstock.goods_sid = goodsnstock.sid
        |
      """.stripMargin


    val result = SqlHelper.getInputOutputTable(selectSql)
    val expectResult = (List(("recommendation.goods_basic_info", Oper.SELECT), ("sourcedata.s06_pcm_chan_sale", Oper.SELECT),
      ("sourcedata.s06_pcm_picture", Oper.SELECT), ("sourcedata.s06_pcm_picture_url", Oper.SELECT), ("sourcedata.s06_pcm_stock", Oper.SELECT)), Nil)
    assert(result == expectResult)
  }

  @Test
  def dropTest = {
    val dropSQL = "drop table recommendation.tmp_price1"
    val result = SqlHelper.getInputOutputTable(dropSQL)
    val expectResult = (List(("recommendation.tmp_price1", Oper.DROP)), Nil)
    assert(result == expectResult)

  }


  @Test
  def createAsSelectTest = {
    val createSQL =
      """
        |create table recommendation.goods_basic_info_temp as
        |select prd_basic.*, gprice.sale_price, gprice.channel_sid
        |from (
        | select goods_prod_brand.*, prod_cat.category_id,prod_cat.category_name
        | from (
        |   select goods.*,pmbi.cn_name
        |   from (
        |     select SID,MDM_GOODS_SID,GOODS_SALES_NAME,GOODS_TYPE,PRO_SID,brand_sid,logistics_type,yun_type,store_sid,com_sid
        |     from sourcedata.s06_pcm_mdm_goods) goods
        |     left join sourcedata.s06_pcm_mdm_brand_info pmbi on pmbi.sid = goods.brand_sid) goods_prod_brand
        |     left join (
        |         select distinct product_id, category_id,category_name
        |         from idmdata.dim_management_category) prod_cat on prod_cat.product_id = goods_prod_brand.PRO_SID) prd_basic join
        |recommendation.goods_price  gprice on prd_basic.sid = gprice.goods_sid
        |
      """.stripMargin

    val expectResult = (List(("idmdata.dim_management_category", Oper.SELECT), ("recommendation.goods_price", Oper.SELECT),
      ("sourcedata.s06_pcm_mdm_brand_info", Oper.SELECT), ("sourcedata.s06_pcm_mdm_goods", Oper.SELECT)),
      List(("recommendation.goods_basic_info_temp", Oper.CREATE_TABLE)))
    val result = SqlHelper.getInputOutputTable(createSQL)
    assert(expectResult == result)

  }

  @Test
  def createTest2 = {
    val createSQL =
      """
        |CREATE TABLE recommendation.dim_category(
        |        category_id bigint,
        |  category_name string,
        |  level1_id bigint,
        |  level1_name string,
        |  level2_id bigint,
        |  level2_name string,
        |  level3_id bigint,
        |  level3_name string)
        |ROW FORMAT DELIMITED
        |  FIELDS TERMINATED BY '\t'
        |STORED AS INPUTFORMAT
        |  'org.apache.hadoop.mapred.TextInputFormat'
        |OUTPUTFORMAT
        |  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
      """.stripMargin

    val result = SqlHelper.getInputOutputTable(createSQL)
    val expectResult = (Nil, List(("recommendation.dim_category", Oper.CREATE_TABLE)))
    assert(result == expectResult)

  }

  @Test
  def insertIntoSelectTest = {
    val insertIntoSQL =
      """
        |insert into table recommendation.dim_category
        | select category_id,category_name,level1_id,level1_name,level2_id,level2_name,level3_id,level3_name
        |  from idmdata.dim_management_category
        | where validate_flag ='1'
        | group by category_id,category_name,level1_id,level1_name,level2_id,level2_name,level3_id,level3_name
        | order by category_id
        |
      """.stripMargin

    val result = SqlHelper.getInputOutputTable(insertIntoSQL)
    val expectResult = (List(("idmdata.dim_management_category", Oper.SELECT)), List(("recommendation.dim_category", Oper.INSERT)))
    assert(result == expectResult)


  }

  @Test
  def alterTableTest = {
    val alterSQL = "alter table recommendation.goods_price_temp  rename to recommendation.goods_price"
    val result = SqlHelper.getInputOutputTable(alterSQL)
    val expectResult = (List(("recommendation.goods_price_temp", Oper.ALTER)), List(("recommendation.goods_price", Oper.ALTER_TABLE)))
    assert(result == expectResult)

  }







}
