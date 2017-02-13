package com.bl.bd.rpc


/**
  * Created by MK33 on 2016/11/24.
  */
class RpcRequest {
  var requestId: String = null
  var interfaceName: String = null
  var serviceVersion: String = null
  var methodName: String = null
  var parameterTypes: scala.Array[Class[_]] = null
  var parameters: Array[AnyRef] = null
}
