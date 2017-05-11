package exonode.clifton.config

import java.io.File

import exonode.clifton.config.ProtocolConfig.{Hour, Min}

import scala.util.{Success, Try}

/**
  * Created by #GrowinScala
  *
  * Loads the config file that has the constants used by the nodes.
  */
object ConfigLoader {

  private val CONFIG_FILE: String = "config.ini"
  private val configConstants: Map[String, Long] = loadConfigurationFile()

  def getOrDefault(constantName: String, defaultValue: Long): Long =
    configConstants.getOrElse(constantName, defaultValue)

  private def loadConfigurationFile(): Map[String, Long] = {
    if (!new File(CONFIG_FILE).exists())
      Map()
    else {
      val config = clearComments(scala.io.Source.fromFile(CONFIG_FILE).mkString).split('\n')
      config.foldLeft(Map[String, Long]()) {
        (map, line) =>
          if (line.contains("=")) {
            Try {
              val sign = line.indexOf("=")
              val (constant, time) = line.splitAt(sign)
              constant.trim -> time.substring(1).trim.toLong
            } match {
              case Success(value) => map + value
              case _ => map
            }
          } else {
            map
          }
      }
    }
  }

  private def clearComments(file: String): String = {
    file.split("\n").map(str => {
      val index = str.indexOf("//")
      if (index == -1) str
      else str.substring(0, index)
    }).map(str => str.filterNot(_ == '\r')).mkString("\n")
  }

  val Default: ProtocolConfig = new ProtocolConfigDefault

  class ProtocolConfigDefault extends ProtocolConfig {
    // Lease times
    override val DataLeaseTime: Long = getOrDefault("DATA_LEASE_TIME", 1 * Hour)
    override val NodeInfoLeaseTime: Long = getOrDefault("NODE_INFO_LEASE_TIME", Min)
    override val TableLeaseTime: Long = getOrDefault("TABLE_LEASE_TIME", Min)

    // Check space for updates
    override val TableUpdateTime: Long = getOrDefault("TABLE_UPDATE_TIME", 2 * 1000)
    override val NodeCheckTableTime: Long = getOrDefault("NODE_CHECK_TABLE_TIME", 30 * 1000)
    override val AnalyserCheckGraphsTime: Long = getOrDefault("ANALYSER_CHECK_GRAPHS_TIME", 20 * 1000)

    // Other times
    override val NodeMinSleepTime: Long = getOrDefault("NODE_MIN_SLEEP_TIME", 250)
    override val NodeMaxSleepTime: Long = getOrDefault("NODE_MAX_SLEEP_TIME", 5 * 1000)
    override val AnalyserSleepTime: Long = getOrDefault("ANALYSER_SLEEP_TIME", 5 * 1000)
    override val ErrorSleepTime: Long = getOrDefault("ERROR_SLEEP_TIME", 30 * 1000)

    // Consensus constants
    override val ConsensusEntriesToRead: Int = getOrDefault("CONSENSUS_ENTRIES_TO_READ", 3).toInt
    override val ConsensusLoopsToFinish: Int = getOrDefault("CONSENSUS_LOOPS_TO_FINISH", 3).toInt

    override val ConsensusTestTableExistTime: Long = getOrDefault("CONSENSUS_TEST_TABLE_EXIST_TIME", 1000)

    // (should change with the amount of nodes in space: more nodes -> more time)
    override val ConsensusWantAnalyserTime: Long = getOrDefault("CONSENSUS_WANT_TBA_LEASE_TIME", 1 * Min)

    override val ConsensusMinSleepTime: Long = getOrDefault("CONSENSUS_MIN_SLEEP_TIME", 2 * 1000)
    override val ConsensusMaxSleepTime: Long = getOrDefault("CONSENSUS_MAX_SLEEP_TIME", 4 * 1000)

    override val BackupDataLeaseTime: Long = getOrDefault("BACKUP_DATA_LEASE_TIME", 60 * Min)
    override val BackupTimeoutTime: Long = getOrDefault("BACKUP_TIMEOUT_TIME", 9 * Min)
  }

}
