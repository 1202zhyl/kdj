package com.bl.bd.dao

import java.sql.DriverManager

import com.bl.bd.util.Configuration

/**
  * 从 oracle 数据库获取a线下门店 store 的列表，测试环境连接不了生产数据库，在本地把生产数据库导入到测试环境吧
  * Created by MK33 on 2016/10/12.
  */
object GetStoreList {

  private var shopList: List[(String, String, String, String)] = Nil


  def main(args: Array[String]): Unit = {
    val shops = getStoreList()
    shops.foreach(println)
    println("shop count: " + shops.length)
  }

  /**
    * get stores information from database
    *
    * @return List((storeName, storeLocation, storeLngLat, storeScope))
    */
  def getStoreList(): scala.collection.immutable.List[(String, String, String, String)] = {
    if (shopList == null || shopList.isEmpty) fetch()
    shopList
  }

  private def fetch(): Unit = {
    val confMap = Configuration.getConf("RDB.properties")
    val jdbc = confMap("jdbc")
//    Class.forName("oracle.jdbc.driver.OracleDriver")
    Class.forName(jdbc)

//    val url = "jdbc:oracle:thin:@10.201.48.18:1521:report"
    val url = confMap("url")
    val username = confMap("user")
    val password = confMap("passwd")

    val conn = DriverManager.getConnection(url, username, password)
    val stmt = conn.createStatement()
    val result = stmt.executeQuery(" select store_name, store_location, store_lnglat, store_scope  from idmdata.dim_site_scope ")
    while (result.next()) {
      val storeName = result.getString(1)
      val storeLocation = result.getString(2)
      val storeLngLat = result.getString(3)
      val storeScope = result.getString(4)
      shopList = (storeName, storeLocation, storeLngLat, storeScope) :: shopList
    }
  }


}
