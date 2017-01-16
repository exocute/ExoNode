package exonode.clifton.signals

import exonode.clifton.node.Log.{Info, LogLevel}

/**
  * Created by #ScalaTeam on 20/12/2016.
  *
  * Transports the information of  a log message
  */
class LoggingSignal(private var logMessage: String, private var logLevel: LogLevel = Info) extends Serializable {

  def this() = this("")

  def getLogLevel: LogLevel = logLevel

  def setLogLevel(logLevel: LogLevel): Unit = this.logLevel = logLevel

  def getLogMessage: String = logMessage

  def setLogMessage(logMessage: String): Unit = this.logMessage = logMessage
}
