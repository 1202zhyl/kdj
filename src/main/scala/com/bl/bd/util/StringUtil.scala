package com.bl.bd.util

import org.apache.commons.lang3.StringUtils

/**
  * Created by MK33 on 2016/11/24.
  */
object StringUtil {

  def isEmpty(str: String): Boolean = {
    if (str != null) {
      StringUtils.isEmpty(str.trim)
    } else true
  }

  def isNotEmpty(str: String): Boolean = {
    return !isEmpty(str)
  }

  def split(str: String, separator: String): Array[String] = {
    return StringUtils.splitByWholeSeparator(str, separator)
  }
}

