//package com.bl.bd.rpc
//
//import io.netty.bootstrap.Bootstrap
//import io.netty.channel._
//import io.netty.channel.nio.NioEventLoopGroup
//import io.netty.channel.socket.SocketChannel
//import io.netty.channel.socket.nio.NioSocketChannel
//import org.apache.spark.network.protocol.RpcResponse
//import org.slf4j.{Logger, LoggerFactory}
///**
//  * Created by MK33 on 2016/11/24.
//  */
//
//class RpcClient extends SimpleChannelInboundHandler[RpcResponse] {
//  private val LOGGER: Logger = LoggerFactory.getLogger (classOf[RpcClient] )
//
//  private val host: String = null
//  private val port: Int = 0
//
//  private var response: RpcResponse = null
//
//  def this (host: String, port: Int) {
//  this ()
//  this.host = host
//  this.port = port
//}
//
//  @throws[Exception]
//  def channelRead0 (ctx: ChannelHandlerContext, response: RpcResponse) {
//  this.response = response
//}
//
//  @throws[Exception]
//  override def exceptionCaught (ctx: ChannelHandlerContext, cause: Throwable) {
//  LOGGER.error ("api caught exception", cause)
//  ctx.close
//}
//
//  @throws[Exception]
//  def send (request: RpcRequest): RpcResponse = {
//  val group: EventLoopGroup = new NioEventLoopGroup
//  try {
//  val bootstrap: Bootstrap = new Bootstrap
//  bootstrap.group (group)
//  bootstrap.channel (classOf[NioSocketChannel] )
//  bootstrap.handler (new ChannelInitializer[SocketChannel] () {
//  @throws[Exception]
//  def initChannel (channel: SocketChannel) {
//  val pipeline: ChannelPipeline = channel.pipeline
//  pipeline.addLast (new RpcEncoder (classOf[RpcRequest] ) )
//  pipeline.addLast (new RpcDecoder (classOf[RpcResponse] ) )
//  pipeline.addLast (thisRpcClient)
//}
//})
//  bootstrap.option (ChannelOption.TCP_NODELAY, true)
//  val future: ChannelFuture = bootstrap.connect (host, port).sync
//  val channel: Channel = future.channel
//  channel.writeAndFlush (request).sync
//  channel.closeFuture.sync
//  return response
//} finally {
//  group.shutdownGracefully
//}
//}
//}
