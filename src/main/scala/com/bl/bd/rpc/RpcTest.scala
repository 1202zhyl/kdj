package com.bl.bd.rpc

import com.xxx.rpc.client.RpcProxy
import com.xxx.rpc.sample.api.AMapService
import com.xxx.rpc.sample.server.AMapServiceImpl

/**
  * Created by MK33 on 2016/11/24.
  */
object RpcTest {

  def main(args: Array[String]) {
    val aMapService = new AMapServiceImpl

    val request: String = "http://restapi.amap.com/v3/geocode/geo?key=02ea79be41a433373fc8708a00a2c0fa&address=上海市市辖区宝山区四川南路26号6楼"
    val start = System.currentTimeMillis()
    val result: String = aMapService.request(request)
    println(System.currentTimeMillis() - start)
    System.out.println(result)
    val start2 = System.currentTimeMillis()
    println(aMapService.request("http://restapi.amap.com/v3/geocode/geo?key=02ea79be41a433373fc8708a00a2c0fa&address=上海市四川中路"))
    println(System.currentTimeMillis() - start2)

  }

}
