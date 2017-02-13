package com.bl.bd.util

import java.io.{BufferedReader, InputStreamReader, LineNumberInputStream, LineNumberReader}

/**
  * Created by MK33 on 2016/12/7.
  */
object ShellUtil {

  def runShell(shell: String): Int = {
    val process = Runtime.getRuntime.exec(shell)
    val in = process.getInputStream
    val buffer = new BufferedReader(new InputStreamReader(in))
    val errorBuffer = new BufferedReader(new InputStreamReader(process.getErrorStream))
    var tmp = buffer.readLine()
    var tmp2 = errorBuffer.readLine()

    while (tmp != null || tmp2 != null) {
      if (tmp != null) {
        println(tmp)
        tmp = buffer.readLine()
      }
      if (tmp2 != null) {
        println(tmp2)
        tmp2 = errorBuffer.readLine()
      }
    }

    process.waitFor()
  }

}
