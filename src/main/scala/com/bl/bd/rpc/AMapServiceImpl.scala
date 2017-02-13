package com.bl.bd.rpc

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.{HttpURLConnection, URL}

/**
  * Created by MK33 on 2016/11/24.
  */
object AMapServiceImpl extends AMapService{

  override def request(url: String): String = {
    val u = new URL(url)
    val httpURLConnection = u.openConnection.asInstanceOf[HttpURLConnection]
    val in = httpURLConnection.getInputStream
    val bf = new BufferedReader(new InputStreamReader(in))
    val sb = new StringBuffer
    var tmp = bf.readLine
    while (tmp != null) {
      {
        sb.append(tmp)
        tmp = bf.readLine
      }
    }
    sb.toString
  }
}
