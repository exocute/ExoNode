package exonode.clifton.signals

/**
  * Created by #ScalaTeam on 20/12/2016.
  *
  * Transports the information of a log message
  */
case class LoggingSignal(code: Int, level: String, nodeID: String,
                         graphID: String, actIDfrom: String, actIDto: String,
                         injID: String, message: String, time: Long) {

  def setMessage(value: String): LoggingSignal = LoggingSignal(code, level, nodeID, graphID, actIDfrom, actIDto, injID, value, time)

  def setInjID(value: String): LoggingSignal = LoggingSignal(code, level, nodeID, graphID, actIDfrom, actIDto, value, message, time)

  def setActTo(value: String): LoggingSignal = LoggingSignal(code, level, nodeID, graphID, actIDfrom, value, injID, message, time)

  def setActFrom(value: String): LoggingSignal = LoggingSignal(code, level, nodeID, graphID, value, actIDto, injID, message, time)

  def setGraphID(value: String): LoggingSignal = LoggingSignal(code, level, nodeID, value, actIDfrom, actIDto, injID, message, time)

  def setNodeID(value: String): LoggingSignal = LoggingSignal(code, level, value, graphID, actIDfrom, actIDto, injID, message, time)

  def setLevel(value: String): LoggingSignal = LoggingSignal(code, value, nodeID, graphID, actIDfrom, actIDto, injID, message, time)

  def setCode(value: Int): LoggingSignal = LoggingSignal(value, level, nodeID, graphID, actIDfrom, actIDto, injID, message, time)

}