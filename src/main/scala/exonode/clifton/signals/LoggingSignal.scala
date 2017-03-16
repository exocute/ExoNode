package exonode.clifton.signals

/**
  * Created by #GrowinScala
  *
  * Transports the information of a log message
  */
case class LoggingSignal(code: Int, level: String, nodeID: String,
                         graphID: String, actIDfrom: String, actIDto: String,
                         injID: String, message: String, processingTime: Long) {

  def setMessage(value: String): LoggingSignal = LoggingSignal(code, level, nodeID, graphID, actIDfrom, actIDto, injID, value, processingTime)

  def setInjID(value: String): LoggingSignal = LoggingSignal(code, level, nodeID, graphID, actIDfrom, actIDto, value, message, processingTime)

  def setActTo(value: String): LoggingSignal = LoggingSignal(code, level, nodeID, graphID, actIDfrom, value, injID, message, processingTime)

  def setActFrom(value: String): LoggingSignal = LoggingSignal(code, level, nodeID, graphID, value, actIDto, injID, message, processingTime)

  def setGraphID(value: String): LoggingSignal = LoggingSignal(code, level, nodeID, value, actIDfrom, actIDto, injID, message, processingTime)

  def setNodeID(value: String): LoggingSignal = LoggingSignal(code, level, value, graphID, actIDfrom, actIDto, injID, message, processingTime)

  def setLevel(value: String): LoggingSignal = LoggingSignal(code, value, nodeID, graphID, actIDfrom, actIDto, injID, message, processingTime)

  def setCode(value: Int): LoggingSignal = LoggingSignal(value, level, nodeID, graphID, actIDfrom, actIDto, injID, message, processingTime)

  override def toString: String = s"$level : $message : $nodeID : $graphID : $actIDfrom : $actIDto : $injID : $processingTime"

}