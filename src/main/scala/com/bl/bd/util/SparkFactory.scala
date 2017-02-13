package com.bl.bd.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.hadoop.mapreduce.lib.input.{SequenceFileInputFormat, TextInputFormat}
import org.apache.mahout.cf.taste.hadoop.EntityEntityWritable
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable
import org.apache.mahout.math.VarIntWritable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MK33 on 2016/4/11.
  */
object SparkFactory {
  private[this] var sc: SparkContext = _
  private[this] var hiveContext: HiveContext = _

  /** */
  def getSparkContext(appName: String = "recommend"): SparkContext = {
    if (sc == null) {
      val sparkConf = new SparkConf().setAppName(appName)
      sc = new SparkContext(sparkConf)
      Message.addMessage("application-id:\t" + sc.applicationId)
      Message.addMessage("application-master:\t" + sc.master)
    }
    sc

  }

  def getSparkSession(): SparkSession = {
    SparkSession.builder().enableHiveSupport().getOrCreate()
  }

    def getSparkContext: SparkContext = {
      if (sc == null) getSparkContext()
      else sc
    }


  def destroyResource(): Unit ={
    if (sc != null) sc.stop()
  }


  def getHiveContext : HiveContext = {
    if (hiveContext != null) hiveContext
    else if (hiveContext == null && sc != null) {hiveContext = new HiveContext(sc); hiveContext}
    else {
      sc = getSparkContext
      hiveContext = new HiveContext(sc)
      hiveContext
    }
  }



}
