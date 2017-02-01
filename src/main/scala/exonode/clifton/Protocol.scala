package exonode.clifton

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
  val NODE_SIGNAL_MARKER = "NODE"
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

  val MAX_BACKUPS_IN_SPACE = 2

  // Read times
  val ENTRY_READ_TIME = 0L

  // Lease times
  val INJECTOR_LEASE_TIME: Long = 2 * MIN
  val NODE_INFO_LEASE_TIME: Long = MIN
  val DATA_LEASE_TIME: Long = 1 * HOUR
  val LOG_LEASE_TIME: Long = 1 * HOUR
  val TABLE_LEASE_TIME: Long = MIN
  val JAR_LEASE_TIME: Long = 365L * 24 * HOUR
  val BACKUP_LEASE_TIME: Long = 1 * HOUR
  val ACT_SIGNAL_LEASE_TIME: Long = 24 * HOUR

  // Check space for updates
  val TABLE_UPDATE_TIME: Long = 2 * 1000
  val NODE_CHECK_TABLE_TIME: Long = 30 * 1000
  val NOTIFY_GRAPHS_ANALYSER_TIME: Long = 120 * 1000
  val ANALYSER_CHECK_BACKUP_INFO: Long = 10 * MIN

  // Other times
  val NODE_INFO_EXPIRY_TIME: Long = NODE_CHECK_TABLE_TIME * 2
  val NODE_MIN_SLEEP_TIME: Long = 250
  val NODE_STEP_SLEEP_TIME: Long = 250
  val NODE_MAX_SLEEP_TIME: Long = 5 * 1000
  val ACT_NOT_FOUND_SLEEP_TIME: Long = 30 * 1000
  val ANALYSER_SLEEP_TIME: Long = 5 * 1000
  val INITIAL_TABLE_LEASE_TIME: Long = TABLE_LEASE_TIME * 2
  val BACKUP_UPDATE_DATA_TIME: Long = 40 * MIN
  val BACKUP_UPDATE_INFO_TIME: Long = 5 * MIN
  val BACKUP_MAX_LIVE_TIME: Long = 15 * MIN

  // Consensus constants
  val CONSENSUS_ENTRIES_TO_READ = 5
  val CONSENSUS_LOOPS_TO_FINISH = 3

  // (should change with the amount of nodes in space: more nodes -> more time)
  val CONSENSUS_WANT_TBA_LEASE_TIME: Long = 1 * MIN

  val CONSENSUS_MIN_SLEEP_TIME: Long = 3 * 1000
  val CONSENSUS_MAX_SLEEP_TIME: Long = 6 * 1000

  def consensusRandomSleepTime(): Long = {
    CONSENSUS_MIN_SLEEP_TIME + (math.random() * (CONSENSUS_MAX_SLEEP_TIME - CONSENSUS_MIN_SLEEP_TIME)).toLong
  }

}

