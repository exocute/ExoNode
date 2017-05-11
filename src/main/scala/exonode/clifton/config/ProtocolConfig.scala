package exonode.clifton.config

import java.util.UUID

import scala.collection.immutable.HashMap

/**
  * Created by #GrowinScala
  *
  * Contains all the constants used by the nodes.
  */
abstract class ProtocolConfig extends Serializable {

  val Id: String = UUID.randomUUID().toString

  // Lease times
  val DataLeaseTime: Long
  val NodeInfoLeaseTime: Long
  val TableLeaseTime: Long

  // Check space for updates
  val TableUpdateTime: Long
  val NodeCheckTableTime: Long
  val AnalyserCheckGraphsTime: Long

  // Other times
  final def NodeInfoExpiryTime: Long = NodeCheckTableTime * 2

  val NodeMinSleepTime: Long
  val NodeMaxSleepTime: Long
  val AnalyserSleepTime: Long
  val ErrorSleepTime: Long

  // Consensus constants
  val ConsensusEntriesToRead: Int
  val ConsensusLoopsToFinish: Int

  val ConsensusTestTableExistTime: Long

  // (should change with the amount of nodes in space: more nodes -> more time)
  val ConsensusWantAnalyserTime: Long

  val ConsensusMinSleepTime: Long
  val ConsensusMaxSleepTime: Long

  def consensusRandomSleepTime(): Long =
    ConsensusMinSleepTime + (math.random() * (ConsensusMaxSleepTime - ConsensusMinSleepTime)).toLong

  // Backups constants
  val BackupDataLeaseTime: Long

  final def RenewBackupEntriesTime: Long = BackupDataLeaseTime / 3 * 2

  final def MaxBackupsInSpace: Long = 1 + BackupDataLeaseTime / RenewBackupEntriesTime

  val BackupTimeoutTime: Long

  final def AnalyserCheckBackupInfo: Long = BackupTimeoutTime / 3

  final def SendStillProcessingTime: Long = BackupTimeoutTime / 3
}

object ProtocolConfig {
  final val Min: Long = 60 * 1000
  final val Hour: Long = 60 * Min

  final val LogLeaseTime: Long = 1 * Hour

  val InjectSignalMarker = ">"
  val CollectSignalMarker = "<"
  val LogMarker = "LOG"
  val InfoMarker = "INFO"
  val TableMarker = "TABLE"
  val NodeSignalMarker = "NODESIGNAL"
  val NotProcessingMarker = "NOT_PROCESSING"
  val WantToBeAnalyserMarker = "WANT_TO_BE_ANALYSER"
  val ConfigMarker = "CONFIG"

  val AnalyserMarker = "@"
  val UndefinedActId = "?"

  val LogSeparator = ";"

  type TableType = HashMap[String, Int]

  case class AnalyserTable(table: TableType, config: ProtocolConfig)

  val EmptyTable: TableType = HashMap[String, Int]()

  /**
    * Representation of the node state
    */
  case class NodeInfoType(nodeId: String, activityId: String, dataId: Option[(String, String)])

  val Default = ConfigLoader.Default

}