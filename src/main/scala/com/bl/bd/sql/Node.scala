package com.bl.bd.sql

import scala.collection.mutable

/**
  * Created by MK33 on 2016/12/27.
  */
case class Node(sql: String, dependency: Set[String] = null, output: String = null, description: String = null) {

  var from: mutable.HashSet[Node] = null
  var to: mutable.HashSet[Node] = null
}
