package exonode.clifton.node

import exonode.clifton.config.ProtocolConfig
import exonode.clifton.node.entries.ExoEntry
import exonode.clifton.signals.LoggingSignal

/**
  * Created by #GrowinScala
  *
  * Responsible for writing logs in the space.
  * It allows for different level of logs: INFO, WARN and ERROR
  */
object Log {

  val INFO = "INFO"
  val ERROR = "ERROR"
  val WARN = "WARN"
  val ND = "NOT_DEFINED"

  private val space = SpaceCache.getSignalSpace

  def writeLog(log: LoggingSignal): Long = {
    if(log.level.equals(ERROR)) {
      val newMessage = formatMessage(log.message)
      space.write(ExoEntry(ProtocolConfig.LOG_MARKER, log.setMessage(newMessage)), ProtocolConfig.LOG_LEASE_TIME)
    } else space.write(ExoEntry(ProtocolConfig.LOG_MARKER, log), ProtocolConfig.LOG_LEASE_TIME)
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
