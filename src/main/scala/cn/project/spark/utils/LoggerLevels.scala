package cn.project.spark.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging

object LoggerLevels extends Logging{
  def setStreamingLogLevels(): Unit = {
    val log3jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if(!log3jInitialized) {
      logInfo("Setting log level to [WARN] for streaming example"+
        "To override add a custom log4j.properties to the classpath"
      )
      Logger.getRootLogger.setLevel(Level.ERROR)
    }
  }
}
