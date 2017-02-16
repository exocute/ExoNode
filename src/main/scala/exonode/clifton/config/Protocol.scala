package exonode.clifton.config

import exonode.clifton.config.ConfigLoader._

import scala.collection.immutable.HashMap

/**
  * Created by #ScalaTeam on 02-01-2017
  */
object Protocol {

  private val MIN = 60 * 1000
  private val HOUR = 60 * MIN

  val INJECT_SIGNAL_MARKER = ">"
  val COLLECT_SIGNAL_MARKER = "<"
  val LOG_MARKER = "LOG"
  val INFO_MARKER = "INFO"
  val TABLE_MARKER = "TABLE"
  val NODE_SIGNAL_MARKER = "NODESIGNAL"
  val GRAPH_MARKER = "GRAPH"
  val NOT_PROCESSING_MARKER = "NOT_PROCESSING"
  val WANT_TO_BE_ANALYSER_MARKER = "WANT_TO_BE_ANALYSER"

  val ANALYSER_MARKER = "@"
  val UNDEFINED_ACT_ID = "?"

  val LOG_SEPARATOR = ";"

  type TableType = HashMap[String, Int]
  lazy val EMPTY_TABLE: TableType = HashMap[String, Int]()
  //(activityId, number of activities)
  type TableEntryType = (String, Int)
  //(nodeId, activityId, injectID)
  type NodeInfoType = (String, String, String)
  //(graphId, list of activity ids)
  type GraphEntryType = (String, Vector[String])

  //other constants

  // Read times
  val ENTRY_READ_TIME: Int = getOrDefault("ENTRY_READ_TIME", 0).toInt

  // Lease times
  val INJECTOR_LEASE_TIME: Long = getOrDefault("INJECTOR_LEASE_TIME", 2 * MIN)
  val NODE_INFO_LEASE_TIME: Long = getOrDefault("NODE_INFO_LEASE_TIME", MIN)
  val DATA_LEASE_TIME: Long = getOrDefault("DATA_LEASE_TIME", 1 * HOUR)
  val LOG_LEASE_TIME: Long = getOrDefault("LOG_LEASE_TIME", 1 * HOUR)
  val TABLE_LEASE_TIME: Long = getOrDefault("TABLE_LEASE_TIME", MIN)

  // Check space for updates
  val TABLE_UPDATE_TIME: Long = getOrDefault("TABLE_UPDATE_TIME", 2 * 1000)
  val NODE_CHECK_TABLE_TIME: Long = getOrDefault("NODE_CHECK_TABLE_TIME", 30 * 1000)
  val NOTIFY_GRAPHS_ANALYSER_TIME: Long = getOrDefault("NOTIFY_GRAPHS_ANALYSER_TIME", 2 * MIN)
  val ANALYSER_CHECK_GRAPHS: Long = getOrDefault("ANALYSER_CHECK_GRAPHS", 1 * MIN)

  // Other times
  val NODE_INFO_EXPIRY_TIME: Long = getOrDefault("NODE_INFO_EXPIRY_TIME", NODE_CHECK_TABLE_TIME * 2)
  val NODE_MIN_SLEEP_TIME: Long = getOrDefault("NODE_MIN_SLEEP_TIME", 250)
  val NODE_STEP_SLEEP_TIME: Long = getOrDefault("NODE_STEP_SLEEP_TIME", 250)
  val NODE_MAX_SLEEP_TIME: Long = getOrDefault("NODE_MAX_SLEEP_TIME", 5 * 1000)
  val ANALYSER_SLEEP_TIME: Long = getOrDefault("ANALYSER_SLEEP_TIME", 5 * 1000)
  val ANALYSER_HANDLE_SIGNALS_TIME: Long = getOrDefault("ANALYSER_HANDLE_SIGNALS_TIME", 5 * 1000)
  val ERROR_SLEEP_TIME: Long = getOrDefault("ERROR_SLEEP_TIME", 30 * 1000)

  // Consensus constants
  val CONSENSUS_ENTRIES_TO_READ: Int = getOrDefault("CONSENSUS_ENTRIES_TO_READ", 3).toInt
  val CONSENSUS_LOOPS_TO_FINISH: Int = getOrDefault("CONSENSUS_LOOPS_TO_FINISH", 3).toInt

  val CONSENSUS_TEST_TABLE_EXIST_TIME: Long = getOrDefault("CONSENSUS_TEST_TABLE_EXIST_TIME", 2 * 1000)

  // (should change with the amount of nodes in space: more nodes -> more time)
  val CONSENSUS_WANT_TBA_LEASE_TIME: Long = getOrDefault("CONSENSUS_WANT_TBA_LEASE_TIME", 1 * MIN)

  val CONSENSUS_MIN_SLEEP_TIME: Long = getOrDefault("CONSENSUS_MIN_SLEEP_TIME", 2 * 1000)
  val CONSENSUS_MAX_SLEEP_TIME: Long = getOrDefault("CONSENSUS_MAX_SLEEP_TIME", 4 * 1000)

  def consensusRandomSleepTime(): Long = {
    CONSENSUS_MIN_SLEEP_TIME + (math.random() * (CONSENSUS_MAX_SLEEP_TIME - CONSENSUS_MIN_SLEEP_TIME)).toLong
  }

}

