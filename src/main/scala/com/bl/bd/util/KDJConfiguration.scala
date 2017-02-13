package com.bl.bd.util

/**
  * Created by MK33 on 2016/12/1.
  */
object KDJConfiguration {

  private lazy val props = FileUtil.getPropertiesMap("kdj.properties")

  def getConf(key: String) = props.get(key)




}
