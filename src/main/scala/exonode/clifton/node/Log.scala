package exonode.clifton.node

import exonode.clifton.config.Protocol._
import exonode.clifton.node.entries.ExoEntry
import exonode.clifton.signals.{LoggingSignal}

/**
  * Created by #ScalaTeam on 20/12/2016.
  *
  * Responsible for writing logs in the space.
  * It allows for different level of logs: INFO, WARN and ERROR
  */
object Log {
/*
  sealed trait LogLevel

  case object Info extends LogLevel {
    override def toString: String = "INFO"
  }

  case object Warn extends LogLevel {
    override def toString: String = "WARN"
  }

  case object Error extends LogLevel {
    override def toString: String = "ERROR"
  }*/

  val INFO = "INFO"
  val ERROR = "ERROR"
  val WARN = "WARN"
  val ND = "NOT_DEFINED"

  private val space = SpaceCache.getSignalSpace

/*  private def sendMessage(id: String, msg: String, logLevel: LogLevel): Unit = {
    val logSignal = LoggingSignalOld(id + LOG_SEPARATOR + msg, logLevel)
    space.write(ExoEntry(LOG_MARKER, logSignal), LOG_LEASE_TIME)
  }

  def info(id: String, msg: String): Unit = sendMessage(id, msg, Info)

  def warn(id: String, msg: String): Unit = sendMessage(id, msg, Warn)

  def error(id: String, msg: String): Unit = sendMessage(id, formatMessage(msg), Error)*/

  def receiveLog(log: LoggingSignal) = {
    if(log.level.equals(ERROR)) {
      val newMessage = formatMessage(log.message)
      space.write(ExoEntry(LOG_MARKER, log.setMessage(newMessage)), LOG_LEASE_TIME)
    } else space.write(ExoEntry(LOG_MARKER, log), LOG_LEASE_TIME)
  }

  private def formatMessage(msg: String): String = {
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
