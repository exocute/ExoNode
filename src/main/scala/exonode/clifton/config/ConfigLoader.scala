package exonode.clifton.config

import java.io.File

import exonode.clifton.config.ProtocolConfig.{HOUR, MIN}

import scala.util.{Success, Try}

/**
  * Created by #GrowinScala
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

  val DEFAULT: ProtocolConfig = new ProtocolConfigDefault

  class ProtocolConfigDefault extends ProtocolConfig {
    // Lease times
    override val DATA_LEASE_TIME: Long = getOrDefault("DATA_LEASE_TIME", 1 * HOUR)
    override val NODE_INFO_LEASE_TIME: Long = getOrDefault("NODE_INFO_LEASE_TIME", MIN)
    override val TABLE_LEASE_TIME: Long = getOrDefault("TABLE_LEASE_TIME", MIN)

    // Check space for updates
    override val TABLE_UPDATE_TIME: Long = getOrDefault("TABLE_UPDATE_TIME", 2 * 1000)
    override val NODE_CHECK_TABLE_TIME: Long = getOrDefault("NODE_CHECK_TABLE_TIME", 30 * 1000)
    override val ANALYSER_CHECK_GRAPHS_TIME: Long = getOrDefault("ANALYSER_CHECK_GRAPHS_TIME", 20 * 1000)

    // Other times
    override val NODE_MIN_SLEEP_TIME: Long = getOrDefault("NODE_MIN_SLEEP_TIME", 250)
    override val NODE_MAX_SLEEP_TIME: Long = getOrDefault("NODE_MAX_SLEEP_TIME", 5 * 1000)
    override val ANALYSER_SLEEP_TIME: Long = getOrDefault("ANALYSER_SLEEP_TIME", 5 * 1000)
    override val ERROR_SLEEP_TIME: Long = getOrDefault("ERROR_SLEEP_TIME", 30 * 1000)

    // Consensus constants
    override val CONSENSUS_ENTRIES_TO_READ: Int = getOrDefault("CONSENSUS_ENTRIES_TO_READ", 3).toInt
    override val CONSENSUS_LOOPS_TO_FINISH: Int = getOrDefault("CONSENSUS_LOOPS_TO_FINISH", 3).toInt

    override val CONSENSUS_TEST_TABLE_EXIST_TIME: Long = getOrDefault("CONSENSUS_TEST_TABLE_EXIST_TIME", 1000)

    // (should change with the amount of nodes in space: more nodes -> more time)
    override val CONSENSUS_WANT_TBA_LEASE_TIME: Long = getOrDefault("CONSENSUS_WANT_TBA_LEASE_TIME", 1 * MIN)

    override val CONSENSUS_MIN_SLEEP_TIME: Long = getOrDefault("CONSENSUS_MIN_SLEEP_TIME", 2 * 1000)
    override val CONSENSUS_MAX_SLEEP_TIME: Long = getOrDefault("CONSENSUS_MAX_SLEEP_TIME", 4 * 1000)

    override val BACKUP_DATA_LEASE_TIME: Long = getOrDefault("BACKUP_DATA_LEASE_TIME", 60 * MIN)
    override val BACKUP_TIMEOUT_TIME: Long = getOrDefault("BACKUP_TIMEOUT_TIME", 9 * MIN)
  }

}
