package com.bl.bd.rpc

/**
  * Created by MK33 on 2016/11/24.
  */
trait ServiceDiscovery {
  def discover (serviceName: String): String
}
