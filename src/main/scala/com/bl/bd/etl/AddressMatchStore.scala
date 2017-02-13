package com.bl.bd.etl

import com.bl.bd.kdj.ShopInfo
import com.bl.bd.util.{Configuration, SparkFactory}
import com.infomatiq.jsi.Point
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConversions._

/**
  * Created by MK33 on 2016/12/8.
  */
object AddressMatchStore {

  def main(args: Array[String]) {
    run()
  }

  def run(): Unit = {
    addressMatchStore
  }


  /**  收货地址和线下门店匹配  */
  private def addressMatchStore = {

    val sc = SparkFactory.getSparkContext("address store match")
    //  加载快到家门店列表
    val stores = GetStoreList.getStoreList()
    val storesBrd = sc.broadcast(stores)

    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set(TableInputFormat.INPUT_TABLE, "addressCoordinate")
    val hBaseMap = Configuration.getConf("hbase-site.properties")
    hBaseMap.foreach(s => hBaseConf.set(s._1, s._2))
    val hBaseRawData = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map(_._2)

    hBaseRawData.foreachPartition { partition =>
      val hBaseConf2 = HBaseConfiguration.create()
      val conn = ConnectionFactory.createConnection(hBaseConf2)
      val table = conn.getTable(TableName.valueOf("address_coordinate_store"))
      val shopInfo = new ShopInfo(storesBrd.value)
      val columnFamily = Bytes.toBytes("coordinate")

      partition.foreach { case row =>
        val key = row.getRow
        val latitude = row.getValue(columnFamily, Bytes.toBytes("latitude"))
        val longitude = row.getValue(columnFamily, Bytes.toBytes("longitude"))
        val shopsList = if (longitude != null && latitude != null) {
          val point = new Point(Bytes.toString(longitude).toFloat, Bytes.toString(latitude).toFloat)
          shopInfo.getStores(point)
        } else null
        if (shopsList != null) {
          for ((storeName, storeLocation, lngLat) <- shopsList) {
            val put = new Put(Bytes.toBytes(Bytes.toString(key) + "-" + storeName))
            put.addColumn(columnFamily, Bytes.toBytes("address"), key)
            put.addColumn(columnFamily, Bytes.toBytes("longitude"), row.getValue(Bytes.toBytes("coordinate"), Bytes.toBytes("longitude")))
            put.addColumn(columnFamily, Bytes.toBytes("latitude"), row.getValue(Bytes.toBytes("coordinate"), Bytes.toBytes("latitude")))
            put.addColumn(columnFamily, Bytes.toBytes("province"), row.getValue(Bytes.toBytes("coordinate"), Bytes.toBytes("province")))
            put.addColumn(columnFamily, Bytes.toBytes("city"), row.getValue(Bytes.toBytes("coordinate"), Bytes.toBytes("city")))
            put.addColumn(columnFamily, Bytes.toBytes("citycode"), row.getValue(Bytes.toBytes("coordinate"), Bytes.toBytes("citycode")))
            put.addColumn(columnFamily, Bytes.toBytes("district"), row.getValue(Bytes.toBytes("coordinate"), Bytes.toBytes("district")))
            put.addColumn(columnFamily, Bytes.toBytes("storeName"), Bytes.toBytes(storeName))
            put.addColumn(columnFamily, Bytes.toBytes("storeLocation"), Bytes.toBytes(storeLocation))
            table.put(put)
          }

        }
      }
    }

  }


}
