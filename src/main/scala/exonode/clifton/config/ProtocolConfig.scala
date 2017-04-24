package exonode.clifton.config

import java.util.UUID

import scala.collection.immutable.HashMap

/**
  * Created by #GrowinScala
  */
abstract class ProtocolConfig extends Serializable {

  val ID: String = UUID.randomUUID().toString

  // Lease times
  val DATA_LEASE_TIME: Long
  val NODE_INFO_LEASE_TIME: Long
  val TABLE_LEASE_TIME: Long

  // Check space for updates
  val TABLE_UPDATE_TIME: Long
  val NODE_CHECK_TABLE_TIME: Long
  val ANALYSER_CHECK_GRAPHS_TIME: Long

  // Other times
  final def NODE_INFO_EXPIRY_TIME: Long = NODE_CHECK_TABLE_TIME * 2

  val NODE_MIN_SLEEP_TIME: Long
  val NODE_MAX_SLEEP_TIME: Long
  val ANALYSER_SLEEP_TIME: Long
  val ERROR_SLEEP_TIME: Long

  // Consensus constants
  val CONSENSUS_ENTRIES_TO_READ: Int
  val CONSENSUS_LOOPS_TO_FINISH: Int

  val CONSENSUS_TEST_TABLE_EXIST_TIME: Long

  // (should change with the amount of nodes in space: more nodes -> more time)
  val CONSENSUS_WANT_TBA_LEASE_TIME: Long

  val CONSENSUS_MIN_SLEEP_TIME: Long
  val CONSENSUS_MAX_SLEEP_TIME: Long

  def consensusRandomSleepTime(): Long =
    CONSENSUS_MIN_SLEEP_TIME + (math.random() * (CONSENSUS_MAX_SLEEP_TIME - CONSENSUS_MIN_SLEEP_TIME)).toLong

}

object ProtocolConfig {
  final val MIN: Long = 60 * 1000
  final val HOUR: Long = 60 * MIN

  final val LOG_LEASE_TIME: Long = 1 * HOUR

  val INJECT_SIGNAL_MARKER = ">"
  val COLLECT_SIGNAL_MARKER = "<"
  val LOG_MARKER = "LOG"
  val INFO_MARKER = "INFO"
  val TABLE_MARKER = "TABLE"
  val NODE_SIGNAL_MARKER = "NODESIGNAL"
  val NOT_PROCESSING_MARKER = "NOT_PROCESSING"
  val WANT_TO_BE_ANALYSER_MARKER = "WANT_TO_BE_ANALYSER"
  val CONFIG_MARKER = "CONFIG"

  val LOGCODE_STARTED_NODE = 0
  val LOGCODE_STARTED_GRAPH = 1
  val LOGCODE_CHANGED_ACT = 2
  val LOGCODE_PROCESSING_INPUT = 3
  val LOGCODE_FINISHED_PROCESSING = 4
  val LOGCODE_FAILED = 5
  val LOGCODE_DATA_RECOVERED = 6
  val LOGCODE_INJECTED = 7
  val LOGCODE_COLLECTED = 8
  val LOGCODE_ERROR_PROCESSING = 9
  val LOGCODE_NODE_SHUTDOWN = 10
  val LOGCODE_VALUES_LOST = 11
  val LOGCODE_ACTIVITY_NOT_FOUND = 12
  val LOGCODE_CLASS_NOT_LOADED = 13
  val LOGCODE_INFORMATION_LOST = 14

  val ANALYSER_MARKER = "@"
  val UNDEFINED_ACT_ID = "?"

  val LOG_SEPARATOR = ";"

  type TableType = HashMap[String, Int]

  case class AnalyserTable(table: TableType, config: ProtocolConfig)

  val EMPTY_TABLE: TableType = HashMap[String, Int]()

  case class NodeInfoType(nodeId: String, activityId: String, dataId: Option[(String, String)])

  val DEFAULT = ConfigLoader.DEFAULT

}