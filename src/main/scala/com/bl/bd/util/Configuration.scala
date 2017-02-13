package com.bl.bd.util

import org.apache.hadoop.fs.FileSystem

import scala.collection.mutable

/**
  * Created by MK33 on 2016/11/23.
  */
object Configuration {

  private val confMap = mutable.Map.empty[String, Map[String, String]]
  private var fs: FileSystem = _

  /** get configuration */
  def getConf(name: String): Map[String, String] = {
    if (confMap.contains(name)) {
      confMap(name)
    } else {
      val map = FileUtil.getPropertiesMap(name)
      confMap(name) = map
      map
    }
  }

  def getHadoopFS = {
    if (fs == null) {
      val conf = new org.apache.hadoop.conf.Configuration()
      fs = org.apache.hadoop.fs.FileSystem.get(conf)
    }
    fs
  }

}
