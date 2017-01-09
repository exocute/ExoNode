package exonode.clifton

import scala.collection.immutable.HashMap

/**
  * Created by #ScalaTeam on 02-01-2017.
  */
object Protocol {

  val LOG_MARKER = "LOG"
  val INFO_MARKER = "INFO"
  val TABLE_MARKER = "TABLE"

  val INJECT_SIGNAL_MARKER = ">"
  val COLLECT_SIGNAL_MARKER = "<"

  val ANALISER_ACT_ID = "@"
  val UNDEFINED_ACT_ID = "?"

  type TableType = HashMap[String, Int]
  type TableEntryType = (String, Int)

  // Read times
  val ENTRY_READ_TIME = 0L

  // Lease times
  val NODE_INFO_LEASE_TIME: Long = 5L * 60 * 1000
  val DATA_LEASE_TIME: Long = 1L * 60 * 60 * 1000
  val LOG_LEASE_TIME: Long = 1L * 60 * 60 * 1000
  val TABLE_LEASE_TIME: Long = 60L * 1000
  val JAR_LEASE_TIME: Long = 365L * 24 * 60 * 60 * 1000
  val ACT_SIGNAL_LEASE_TIME: Long = 24L * 60 * 60 * 1000

  // check space for updates
  val NODE_INFO_UPDATE_TIME: Long = 30L * 1000
  val TABLE_UPDATE_TIME: Long = 2L * 1000
  val NODE_CHECK_TABLE_TIME: Long = 30L * 1000

  // other times
  val NODE_INFO_EXPIRY_TIME: Long = NODE_INFO_UPDATE_TIME * 2
  val NODE_MIN_SLEEP_TIME: Long = 500L
  val NODE_MAX_SLEEP_TIME: Long = 3L * 1000
  val ACT_NOT_FOUND_SLEEP_TIME: Long = 30L * 1000
  val ANALISER_SLEEP_TIME: Long = 1L * 1000

}

