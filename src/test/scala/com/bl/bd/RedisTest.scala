package com.bl.bd

import org.junit.Test
import redis.clients.jedis.Jedis
import scala.collection.JavaConversions._

/**
  * Created by MK33 on 2017/1/4.
  */
class RedisTest {

  @Test
  def standaloneTest = {

    val jedis0 = new Jedis("10.201.129.74", 6378)

    jedis0.set("test", "test")
    jedis0.close()

  }

}
