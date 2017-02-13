package com.bl.bd.etl

import com.bl.bd.error.AMapKeyOutDateException
import com.bl.bd.util.{Configuration, SparkFactory}
import com.xxx.rpc.client.RpcProxy
import com.xxx.rpc.sample.api.AMapService
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.json.JSONObject
import org.springframework.context.ApplicationContext
import org.springframework.context.support.ClassPathXmlApplicationContext

/**
  * 将地址转换为经纬度，保存在 HBase 中
  * Created by MK33 on 2016/12/8.
  */
object GetAddressLngLat  {

  def main(args: Array[String]) {
    run()
  }

  def run(): Unit = {
    searchAddressLngLat
  }

  /** 从订单数据中调用接口查询收货地址经纬度 */
  private def searchAddressLngLat = {

    val addressToBeSearchRDD = getAddressToBeSearch
    // 开始调用高德地图接口查询地址经纬度
    addressToBeSearchRDD.foreachPartition { partition =>
      val hBaseConf = HBaseConfiguration.create()
      val hBaseMap = Configuration.getConf("hbase-site.properties")
      hBaseMap.foreach(s => hBaseConf.set(s._1, s._2))
      val connection1 = ConnectionFactory.createConnection(hBaseConf)
      val addressTable = connection1.getTable(TableName.valueOf("address")) // 已经查询成功的地址表
    val addressAndLatLngTable = connection1.getTable(TableName.valueOf("addressCoordinate")) // 地址、经纬度表
    val errorTable = connection1.getTable(TableName.valueOf("addressCoordinate_error")) // 记录出错的数据

      val context: ApplicationContext = new ClassPathXmlApplicationContext("spring.xml")
      val rpcProxy: RpcProxy = context.getBean(classOf[RpcProxy])
      val aMapService: AMapService = rpcProxy.create(classOf[AMapService])

      val url = "http://restapi.amap.com/v3/geocode/geo?key=02ea79be41a433373fc8708a00a2c0fa&address="
      // 调用RPC接口查询地址的经纬度
      partition.foreach { address =>
        try {
          val result = aMapService.request(url + address)
          val json = new JSONObject(result)
          json.getString("status") match {
            case "0" =>
              // 查询失败
              val put = new Put(Bytes.toBytes(address))
              put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("address"), Bytes.toBytes(address))
              put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("result"), Bytes.toBytes(result))
              errorTable.put(put)
            //logger.error("request failed, failed info: " + json.getString("info"))
            case "1" =>
              // 查询成功
              json.getString("infocode") match {
                case "10000" =>
                  // 查询成功
                  val count = json.getString("count").toInt
                  if (count > 1) {
                    // 一个收货地址对应多个经纬度，则视为失败
                    val put = new Put(Bytes.toBytes(address))
                    put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("address"), Bytes.toBytes(address))
                    put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("result"), Bytes.toBytes(result))
                    errorTable.put(put)
                    // logger.info("more than on coordinates: " + result)
                  } else if (count == 1) {
                    // 一个地址只对应一个经纬度，则视为成功，保存在 HBase 中
                    val locationJson = json.getJSONArray("geocodes").getJSONObject(0)
                    val location = locationJson.getString("location").split(",")
                    val put = new Put(Bytes.toBytes(address))
                    put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("longitude"), Bytes.toBytes(location(0)))
                    put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("latitude"), Bytes.toBytes(location(1)))

                    put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("formatted_address"), Bytes.toBytes(locationJson.getString("formatted_address")))
                    put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("province"), Bytes.toBytes(locationJson.getString("province")))
                    put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("city"), Bytes.toBytes(locationJson.getString("city")))
                    if (!locationJson.isNull("citycode")) put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("citycode"), Bytes.toBytes(locationJson.getString("citycode")))
                    put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("district"), Bytes.toBytes(locationJson.getString("district")))
                    put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("adcode"), Bytes.toBytes(locationJson.getString("adcode")))
                    put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("level"), Bytes.toBytes(locationJson.getString("level")))
                    put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("result"), Bytes.toBytes(result))

                    addressAndLatLngTable.put(put)

                    val putAddress = new Put(Bytes.toBytes(address))
                    putAddress.addColumn(Bytes.toBytes("status"), Bytes.toBytes("status"), Bytes.toBytes("OK"))
                    addressTable.put(putAddress)

                  } else {
                    val put = new Put(Bytes.toBytes(address))
                    put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("result"), Bytes.toBytes(result))
                    put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("address"), Bytes.toBytes(address))
                    errorTable.put(put)
                    //logger.info("no coordinate")
                  }
                case "10001" =>
                  val put = new Put(Bytes.toBytes(address))
                  put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("address"), Bytes.toBytes(address))
                  put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("result"), Bytes.toBytes(result))
                  //logger.error("key不正确或过期")
                  throw new AMapKeyOutDateException("key不正确或过期")
                case "10002" =>
                  val put = new Put(Bytes.toBytes(address))
                  put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("address"), Bytes.toBytes(address))
                  put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("result"), Bytes.toBytes(result))
                  errorTable.put(put)
                  //logger.error("没有权限使用相应的服务或者请求接口的路径拼写错误")
                  throw new Exception("没有权限使用相应的服务或者请求接口的路径拼写错误")
                case "10003" =>
                  val put = new Put(Bytes.toBytes(address))
                  put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("address"), Bytes.toBytes(address))
                  put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("result"), Bytes.toBytes(result))
                  errorTable.put(put)
                  //logger.error("访问已超出日访问量")
                  throw new Exception("访问已超出日访问量")
                case e =>
                  val put = new Put(Bytes.toBytes(address))
                  put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("address"), Bytes.toBytes(address))
                  put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("result"), Bytes.toBytes(result))
                  errorTable.put(put)
                  //logger.error("error info code: " + json)
                  throw new Exception("error info code: " + e)
              }
            case e =>
              val put = new Put(Bytes.toBytes(address))
              put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("address"), Bytes.toBytes(address))
              put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("result"), Bytes.toBytes(result))
              errorTable.put(put)
              //logger.error("not known status: " + e)
              throw new Exception("don't know status:" + e)
          }
        } catch {
          case e0: AMapKeyOutDateException =>
            // 可以过期直接停止运行
            throw e0
          case e: Exception =>
          //logger.error(e)
        }

      }

    }

  }


  /** 将收货地址去除空格以后放在 HBase 表中 */
  def getAddressToBeSearch: RDD[String] = {
    val spark = SparkFactory.getHiveContext
    // 将地址中空格去掉
    import spark.sparkSession._
    val addressRDD = spark.sql(
      """
        |select recept_address_detail FROM sourcedata.s03_oms_order_sub where recept_address_detail IS NOT NULL
      """.stripMargin).distinct().rdd.map { row => (row.getString(0).replace(" ", ""), 1)}
    val hBaseConf = HBaseConfiguration.create()
    val hBaseMap = Configuration.getConf("hbase-site.properties")
    hBaseMap.foreach(s => hBaseConf.set(s._1, s._2))
    hBaseConf.set(TableInputFormat.INPUT_TABLE, "address")
    val hBaseRDD = spark.sparkContext.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map(_._2).
      map { row => (Bytes.toString(row.getRow).replace(" ", ""), 1) }
    val addressToBeSearchRDD = addressRDD.subtract(hBaseRDD).map(_._1)
    addressToBeSearchRDD
  }



}
