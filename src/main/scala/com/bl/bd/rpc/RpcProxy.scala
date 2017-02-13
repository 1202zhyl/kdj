//package com.bl.bd.rpc
//
//import java.lang.reflect.{InvocationHandler, Method}
//import java.util.UUID
//
//import com.bl.bd.util.StringUtil
//import com.xxx.rpc.client.RpcClient
//import com.xxx.rpc.common.bean.RpcResponse
//import com.xxx.rpc.common.util.StringUtil
//import com.xxx.rpc.registry.ServiceDiscovery
//
//import scala.reflect.ClassTag
//
///**
//  * Created by MK33 on 2016/11/24.
//  */
//class RpcProxy {
//  var serviceAddress: String = null
//  private var serviceDiscovery: ServiceDiscovery = null
//
//  def create[k](targetClass: Class[k]): ClassTag = {
//    create(targetClass, "")
//  }
//
//  def create[k](targetClass: Class[k], serviceVersion: String): ClassTag = {
//    java.lang.reflect.Proxy.newProxyInstance(targetClass.getClassLoader, Array(targetClass), new InvocationHandler {
//      override def invoke(proxy: scala.Any, method: Method, args: Array[AnyRef]): AnyRef = {
//        val rpcRequest = new RpcRequest()
//        rpcRequest.requestId = UUID.randomUUID().toString
//        rpcRequest.interfaceName = method.getDeclaringClass.getName
//        rpcRequest.serviceVersion = serviceVersion
//        rpcRequest.methodName = method.getName
//        rpcRequest.parameterTypes = method.getParameterTypes.asInstanceOf
//        rpcRequest.parameters = args
//        // 获取 RPC 服务地址
//        if (serviceDiscovery != null) {
//          var serviceName: String = targetClass.getName
//          if (StringUtil.isNotEmpty(serviceVersion)) {
//            serviceName += "-" + serviceVersion
//          }
//          serviceAddress = serviceDiscovery.discover(serviceName)
////          LOGGER.debug("discover service: {} => {}", serviceName, serviceAddress)
//        }
//        if (StringUtil.isEmpty(serviceAddress)) {
//          throw new RuntimeException("server address is empty")
//        }
//
//        // 从 RPC 服务地址中解析主机名与端口号
//        val array: Array[String] = StringUtil.split(serviceAddress, ":")
//        val host: String = array(0)
//        val port: Int = array(1).toInt
//        // 创建 RPC 客户端对象并发送 RPC 请求
//        val client: RpcClient = new RpcClient(host, port)
//        val time: Long = System.currentTimeMillis
//        val response: RpcResponse = client.send(request)
//        LOGGER.debug("time: {}ms", System.currentTimeMillis - time)
//        if (response == null) {
//          throw new RuntimeException("response is null")
//        }
//        // 返回 RPC 响应结果
//        if (response.hasException) {
//          throw response.getException
//        }
//        else {
//          return response.getResult
//        }
//
//      }
//    })
//  }
//
//
//}
