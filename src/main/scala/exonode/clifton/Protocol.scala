package exonode.clifton

import scala.collection.immutable.HashMap

/**
  * Created by #ScalaTeam on 02-01-2017
  */
object Protocol {

  val LOG_MARKER = "LOG"
  val INFO_MARKER = "INFO"
  val TABLE_MARKER = "TABLE"
  val NODE_SIGNAL_MARKER = "NODE"

  val INJECT_SIGNAL_MARKER = ">"
  val COLLECT_SIGNAL_MARKER = "<"

  val ANALYSER_ACT_ID = "@"
  val UNDEFINED_ACT_ID = "?"

  type TableType = HashMap[String, Int]
  //(activityId, number of activities)
  type TableEntryType = (String, Int)
  //(nodeId, activityId) // isBusy in faili
  type NodeInfoType = (String, String)

  // Read times
  val ENTRY_READ_TIME = 0L

  // Lease times
  val INJECTOR_LEASE_TIME: Long = 2 * 60 * 1000
  val NODE_INFO_LEASE_TIME: Long = 60 * 1000
  val DATA_LEASE_TIME: Long = 1 * 60 * 60 * 1000
  val LOG_LEASE_TIME: Long = 1 * 60 * 60 * 1000
  val TABLE_LEASE_TIME: Long = 60 * 1000
  val JAR_LEASE_TIME: Long = 365L * 24 * 60 * 60 * 1000 // Long or overflow
  val ACT_SIGNAL_LEASE_TIME: Long = 24 * 60 * 60 * 1000

  // check space for updates
  val TABLE_UPDATE_TIME: Long = 2 * 1000
  val NODE_CHECK_TABLE_TIME: Long = 30 * 1000

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

}

