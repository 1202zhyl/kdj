package com.bl.bd.util

import java.io.FileNotFoundException
import java.util.Properties

import org.apache.log4j.LogManager
import scala.collection.JavaConversions._

/**
  * Created by MK33 on 2016/11/23.
  */
object FileUtil {

    val logger = LogManager.getLogger(FileUtil.getClass)

    def getPropertiesMap(fileName: String): Map[String, String] = {
      // load properties file
      val inputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream(fileName)

      if (inputStream == null) {
        throw new FileNotFoundException(s"Cannot found ${fileName} file, please make sure this file is in your classpath.")
      }

      val props = new Properties()
      props.load(inputStream)
      inputStream.close()
      logger.info(s"---------  ${fileName}  ----------")
      val properties = for ((k, v) <- props) yield {
        logger.info(k + ":" + v)
        (k, v)
      }
      logger.info("------------------------")
      // kafka client properties
      properties.toMap
    }

}
