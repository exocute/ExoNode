package exonode.clifton.signals.Log

import exonode.clifton.config.ProtocolConfig
import exonode.clifton.node.SpaceCache
import exonode.clifton.node.entries.ExoEntry

/**
  * Created by #GrowinScala
  *
  * Responsible for writing logs in the space.
  * It allows for different level of logs: INFO, WARN and ERROR
  */
object Log {

  private val space = SpaceCache.getSignalSpace

  def writeLog(log: LogType): Long =
    space.write(ExoEntry(ProtocolConfig.LogMarker, log), ProtocolConfig.LogLeaseTime)

  def formatMessage(msg: String): String = {
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

sealed trait LogType extends java.io.Serializable {
  val message: String
  val logType: String
}

sealed trait LogTypeInfo extends LogType {
  override val logType = "Info"
}

sealed trait LogTypeWarn extends LogType {
  override val logType = "Warn"
}

sealed trait LogTypeError extends LogType {
  override val logType = "Error"
}

case class LogStartedNode(nodeId: String) extends LogTypeInfo {
  override val message: String = "Node Started"
}

case class LogStartedGraph(graphId: String, graphName: String) extends LogTypeInfo {
  override val message: String = s"Graph Started - $graphName"
}

case class LogChangedAct(nodeId: String, actIdFrom: String, actIdTo: String, message: String) extends LogTypeInfo

case class LogProcessingInput(nodeId: String,
                              actIdFrom: String,
                              actIdTo: String,
                              injectId: String) extends LogTypeInfo {
  override val message: String = "Node started processing"
}

case class LogFinishedProcessing(nodeId: String,
                                 actIdFrom: String,
                                 actIdTo: String,
                                 injectId: String,
                                 processingTime: Long) extends LogTypeInfo {
  override val message: String = s"Node finished processing in ${processingTime}ms"
}

case class LogDataRecovered(nodeId: String,
                            actIdFrom: String,
                            actIdTo: String,
                            injectId: String) extends LogTypeWarn {
  override val message: String = s"Data with inject id $injectId for activity $actIdTo was recovered successfully"
}

case class LogInjected(injectId: String) extends LogTypeInfo {
  override val message: String = "Injected Input " + injectId
}

case class LogCollected(injectId: String) extends LogTypeInfo {
  override val message: String = "Result Collected " + injectId
}

case class LogErrorProcessing(nodeId: String, message: String) extends LogTypeWarn

case class LogNodeShutdown(nodeId: String, message: String) extends LogTypeWarn

class LogValuesLost(val nodeId: String, val message: String) extends LogTypeError

object LogValuesLost {
  def apply(nodeId: String, message: String) = new LogValuesLost(nodeId, Log.formatMessage(message))

  def unapply(arg: LogValuesLost): Option[(String, String)] = Some(arg.nodeId, arg.message)
}

case class LogActivityNotFound(nodeId: String, activityId: String) extends LogTypeWarn {
  override val message: String = s"ActivitySignal for activity $activityId not found in SignalSpace"
}

case class LogClassNotLoaded(nodeId: String, className: String) extends LogTypeWarn {
  override val message: String = s"Class could not be loaded: $className"
}

case class LogInformationLost(nodeId: String,
                              actIdFrom: String,
                              actIdTo: String,
                              injectId: String) extends LogTypeWarn {
  override val message: String = s"Data with inject id $injectId for activity $actIdTo wasn't recoverable"
}