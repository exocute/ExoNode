package exonode.clifton.signals

import exonode.clifton.node.Log.{Info, LogLevel}

/**
  * Created by #ScalaTeam on 20/12/2016.
  *
  * Transports the information of a log message
  */
case class LoggingSignal(logMessage: String, logLevel: LogLevel) extends Serializable {

  def setLogMessage(newLogMessage: String): LoggingSignal = LoggingSignal(newLogMessage, logLevel)

  def setLogLevel(newLogLevel: LogLevel): LoggingSignal = LoggingSignal(logMessage, newLogLevel)
}
