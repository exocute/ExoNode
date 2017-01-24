package exonode.clifton

import scala.collection.immutable.HashMap

/**
  * Created by #ScalaTeam on 02-01-2017
  */
object Protocol {

  val MIN = 60 * 1000
  val HOUR = 60 * MIN

  val LOG_MARKER = "LOG"
  val INFO_MARKER = "INFO"
  val TABLE_MARKER = "TABLE"
  val NODE_SIGNAL_MARKER = "NODE"
  val GRAPH_MARKER = "GRAPH"
  val NOT_PROCESSING : String = "NOT_PROCESSING"

  val INJECT_SIGNAL_MARKER = ">"
  val COLLECT_SIGNAL_MARKER = "<"

  val ANALYSER_ACT_ID = "@"
  val UNDEFINED_ACT_ID = "?"

  type TableType = HashMap[String, Int]
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

  // check space for updates
  val TABLE_UPDATE_TIME: Long = 2 * 1000
  val NODE_CHECK_TABLE_TIME: Long = 30 * 1000
  val NOTIFY_GRAPHS_ANALYSER_TIME: Long = 120 * 1000
  val ANALYSER_CHECK_BACKUP_INFO: Long = 10*2*1000// * MIN

  // other times
  val NODE_INFO_EXPIRY_TIME: Long = NODE_CHECK_TABLE_TIME * 2
  val NODE_MIN_SLEEP_TIME: Long = 250
  val NODE_STEP_SLEEP_TIME: Long = 250
  val NODE_MAX_SLEEP_TIME: Long = 5 * 1000
  val ACT_NOT_FOUND_SLEEP_TIME: Long = 30 * 1000
  val ANALYSER_SLEEP_TIME: Long = 1 * 1000
  val INITIAL_TABLE_LEASE_TIME: Long = TABLE_LEASE_TIME * 2
  val GRP_CHECKER_TABLE_TIMEOUT: Long = 90 * 1000
  val GRP_CHECKER_SLEEP_TIME: Long = 30 * 1000
  val BACKUP_UPDATE_DATA_TIME: Long = 40 * MIN
  val BACKUP_UPDATE_INFO_TIME: Long = 5*1000// * MIN
  val BACKUP_MAX_LIVE_TIME : Long = 15*2*1000// * MIN


}

