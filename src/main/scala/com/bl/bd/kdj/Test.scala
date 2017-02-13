package com.bl.bd.kdj

import com.bl.bd.dao.GetStoreList
import com.infomatiq.jsi.Point


/**
  * Created by MK33 on 2016/10/13.
  */
object Test {

  def main(args: Array[String]) {
//    Logger.getRootLogger.setLevel(Level.WARN)
    val longitude = 121.443823
    val latitude = 31.265052
    val point = new Point(longitude.toFloat, latitude.toFloat)
    val storeList = GetStoreList.getStoreList()
    val shopInfo = new ShopInfo(storeList)
    println(shopInfo.getStores(longitude.toFloat, latitude.toFloat))
    println(shopInfo.getStores(point))

  }

}
