package exonode.clifton.node

import exonode.clifton.Protocol
import exonode.clifton.signals.LoggingSignal

/**
  * Created by #ScalaTeam on 20/12/2016.
  */
object Log {

  sealed trait LogLevel

  case object Info extends LogLevel {
    override def toString: String = "INFO"
  }

  case object Error extends LogLevel {
    override def toString: String = "ERROR"
  }

  private val log: LoggingSignal = new LoggingSignal

  private val outChannel: OutChannel = new SignalOutChannel(Protocol.LOG_MARKER, Protocol.LOG_LEASE_TIME)

  private def sendMessage(msg: String, logLevel: LogLevel): Unit = {
    log.setLogLevel(logLevel)
    log.setLogMessage(msg)
    outChannel.putObject(log)
  }

  def info(msg: String): Unit = sendMessage(msg, Info)

  def error(msg: String): Unit = sendMessage(formatMessage(msg), Error)

  private var sb = new StringBuilder

  private def formatMessage(msg: String) = {
    // check an exception and catch the resulting stuff
    val stack = new Throwable().getStackTrace
    sb.clear()
    val callerTrace = stack(3)
    sb.append(callerTrace.getClassName)
    sb.append(":")
    sb.append(callerTrace.getMethodName)
    sb.append(":")
    sb.append(callerTrace.getLineNumber)
    sb.append(":")
    sb + msg
  }

}
