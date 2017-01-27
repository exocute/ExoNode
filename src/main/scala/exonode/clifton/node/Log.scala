package exonode.clifton.node

import exonode.clifton.Protocol._
import exonode.clifton.node.entries.ExoEntry
import exonode.clifton.signals.LoggingSignal

/**
  * Created by #ScalaTeam on 20/12/2016.
  *
  * responsible for updating the log file with most relevant info
  * it allows two different writes INFO and ERROR
  */
object Log {

  sealed trait LogLevel

  case object Info extends LogLevel {
    override def toString: String = "INFO"
  }

  case object Error extends LogLevel {
    override def toString: String = "ERROR"
  }

  private val space = SpaceCache.getSignalSpace

  private def sendMessage(msg: String, logLevel: LogLevel): Unit = {
    val logSignal = LoggingSignal(msg, logLevel)
    space.write(ExoEntry(LOG_MARKER, logSignal), LOG_LEASE_TIME)
  }

  def info(msg: String): Unit = sendMessage(msg, Info)

  def error(msg: String): Unit = sendMessage(formatMessage(msg), Error)

  private def formatMessage(msg: String) = {
    // check an exception and catch the resulting stuff
    val stack = new Throwable().getStackTrace
    if (stack.size > 3) {
      val sb = new StringBuilder
      val callerTrace = stack(3)
      sb.append(callerTrace.getClassName)
      sb.append(":")
      sb.append(callerTrace.getMethodName)
      sb.append(":")
      sb.append(callerTrace.getLineNumber)
      sb.append(":")
      sb + msg
    } else
      msg
  }

}
