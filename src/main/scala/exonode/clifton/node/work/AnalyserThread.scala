package exonode.clifton.node.work

import java.text.SimpleDateFormat
import java.util.Date

import exonode.clifton.config.ProtocolConfig
import exonode.clifton.config.ProtocolConfig._
import exonode.clifton.node._
import exonode.clifton.node.entries.{BackupInfoEntry, ExoEntry, GraphEntry}
import exonode.clifton.signals.Log.{Log, _}

import scala.annotation.tailrec
import scala.collection.immutable.HashMap
import scala.language.implicitConversions

/**
  * Created by #GrowinScala
  *
  * AnalyserNode is responsible for updating the space with a table of how many nodes
  * are processing some activity. Analyser Node it's one of the modes of what a clifton node
  * can be transformed.
  */
class AnalyserThread(analyserId: String) extends Thread with BusyWorking with Analyser {

  //the number of entries to be taken from space from a defined time
  private val MaxInfoCalls = 20
  private val MaxGraphs = 100

  private val signalSpace = SpaceCache.getSignalSpace
  private val dataSpace = SpaceCache.getDataSpace

  private type TrackerEntryType = (NodeInfoType, Long)
  private type TrackerTableType = List[TrackerEntryType]
  //((actId, injId, OrderId)), time)
  private type TableBackupType = HashMap[(String, String, String), Long]

  private val templateInfo = ExoEntry[NodeInfoType](ProtocolConfig.InfoMarker, null)
  private val templateTable = ExoEntry[AnalyserTable](ProtocolConfig.TableMarker, null)
  private val templateGraph = GraphEntry(null, null)
  private val templateBackup = BackupInfoEntry(null, null, null, null)
  private val templateConfig = ExoEntry[ProtocolConfig](ProtocolConfig.ConfigMarker, null)

  private var cancel = false

  override def threadIsBusy = true

  override def cancelThread(): Unit = cancel = true

  /**
    * Entry point of the analyser
    */
  override def run(): Unit = {
    try {
      val trackerTable: TrackerTableType = Nil
      val initialTable = reloadGraphs(ProtocolConfig.EmptyTable)
      val config: ProtocolConfig = {
        signalSpace.read(templateConfig, 0) match {
          case None => ProtocolConfig.Default
          case Some(ExoEntry(_, newConfig)) => newConfig
        }
      }
      val entryTable = templateTable.setPayload(AnalyserTable(initialTable, config))
      signalSpace.take(templateTable, 0)
      signalSpace.write(entryTable, config.TableLeaseTime)
      val currentTime = System.currentTimeMillis()
      val backupsTable: TableBackupType = new HashMap[(String, String, String), Long]()
      readInfosFromSpace(config, currentTime, currentTime, currentTime, trackerTable, initialTable, backupsTable)
    } catch {
      case _: Throwable =>
    }
  }

  /**
    * Main method that continues to update the state of the analyser and communicates with the space.
    */
  @tailrec
  private def readInfosFromSpace(config: ProtocolConfig,
                                 lastUpdateTime: Long,
                                 backupsTime: Long,
                                 graphsChangedTime: Long,
                                 trackerTable: TrackerTableType,
                                 originalDistributionTable: TableType,
                                 backupsTable: TableBackupType): Unit = {
    if (!cancel) {
      //RECEIVE:
      //get many NodeInfoType from the signal space

      val infoEntries = signalSpace.takeMany(templateInfo, MaxInfoCalls)

      val currentTime = System.currentTimeMillis()

      val (updatedTrackerTable, newBackupTable) =
        if (infoEntries.nonEmpty) {
          val nodeInfos = infoEntries.map(_.payload).filterNot(_.nodeId == analyserId)
          val (processing, notProcessing) = nodeInfos.partition(_.dataId.isDefined)
          val trackUpdateTable = notProcessing.foldLeft(trackerTable)(
            (tracker, info) => updateTrackerTable(config, tracker, info, currentTime))
          val backupUpdateTable = processing.foldLeft(backupsTable)(
            (backupTable, info) => updateBackupTable(config, backupTable, info, currentTime))
          (trackUpdateTable, backupUpdateTable)
        } else {
          (trackerTable, backupsTable)
        }

      val (updatedDistributionTable, updatedGraphsChangedTime) = {
        val distTable = updateDistributionTable(trackerTable, originalDistributionTable, currentTime)
        if (currentTime - graphsChangedTime > config.AnalyserCheckGraphsTime)
          (reloadGraphs(distTable), currentTime)
        else
          (distTable, graphsChangedTime)
      }

      // Update backup information
      val (updatedBackupsTable, newBackupsTime) =
        if (currentTime - backupsTime > config.AnalyserCheckBackupInfo) {
          val maxNodes = originalDistributionTable.size
          val backupInfos = dataSpace.readMany(templateBackup, updatedTrackerTable.size * maxNodes)
          val notFilterBackupTable = backupInfos.foldLeft(newBackupTable)(
            (table, info) => updateBackup(config, table, info, currentTime))
          val (expiredBackups, backupTable) = notFilterBackupTable.partition(_._2 < currentTime)
          for (((activityTo, injectId, orderId), _) <- expiredBackups)
            recoverData(config, activityTo, injectId, orderId)
          (backupTable, currentTime)
        } else
          (newBackupTable, backupsTime)

      // Send updated distribution table to space
      val (newUpdateTime, finalTrackerTable, finalDistributionTable) =
        if (currentTime - lastUpdateTime >= config.TableUpdateTime) {
          val cleanTrackerTable = cleanExpiredTrackerTable(updatedTrackerTable, currentTime)
          val newDistributionTable = updateDistributionTable(cleanTrackerTable, updatedDistributionTable, currentTime)
          updateTableInSpace(config, newDistributionTable)
          (currentTime, cleanTrackerTable, newDistributionTable)
        } else {
          (lastUpdateTime, updatedTrackerTable, updatedDistributionTable)
        }

      if (infoEntries.size < MaxInfoCalls)
        Thread.sleep(config.AnalyserSleepTime)

      readInfosFromSpace(config, newUpdateTime, newBackupsTime, updatedGraphsChangedTime,
        finalTrackerTable, finalDistributionTable, updatedBackupsTable)
    }
  }

  private def reloadGraphs(distributionTable: TableType): TableType = {
    //    graphsChanged = false
    val graphs = signalSpace.readMany(templateGraph, MaxGraphs)
    (for {
      GraphEntry(graphId, activities) <- graphs
      activityId <- activities
      fullId = graphId + ":" + activityId
    } yield fullId -> {
      distributionTable.get(fullId) match {
        case None => 0
        case Some(v) => v
      }
    }).toSeq
  }

  /**
    * Resets the backup time of an '''NodeInfoType'''
    */
  private def updateBackupTable(config: ProtocolConfig,
                                backupTable: TableBackupType,
                                info: NodeInfoType,
                                currentTime: Long): TableBackupType = {
    val NodeInfoType(_, actId, Some((injectId, orderId))) = info
    val backupEntry = (actId, injectId, orderId)
    if (backupTable.contains(backupEntry)) {
      backupTable.updated(backupEntry, currentTime + config.BackupTimeoutTime)
    } else backupTable
  }

  private def updateBackup(config: ProtocolConfig,
                           table: TableBackupType,
                           info: BackupInfoEntry,
                           currentTime: Long): TableBackupType = {
    val infoBackup = (info.toAct, info.injectId, info.orderId)
    if (table.contains(infoBackup))
      table
    else
      table.updated(infoBackup, currentTime + config.BackupTimeoutTime)
  }

  private def convertToData(activityTo: String, injectId: String, orderId: String): BackupInfoEntry = {
    BackupInfoEntry(activityTo, null, injectId, orderId)
  }

  /**
    * Recovers the data that was send from an activity to another
    */
  private def recoverData(config: ProtocolConfig, activityTo: String, injectId: String, orderId: String): Unit = {
    val backupInfoTemplate = convertToData(activityTo, injectId, orderId)

    def recoverNextEntry(): Unit = {
      dataSpace.take(backupInfoTemplate, 0) match {
        case None => // No backups left
        case Some(backupInfoEntry) =>
          val backupEntryTemplate = backupInfoEntry.createTemplate()
          dataSpace.take(backupEntryTemplate, 0) match {
            case None =>
              // information was lost
              Log.writeLog(LogInformationLost(analyserId, backupInfoEntry.fromAct, activityTo, injectId))
            case Some(backupEntry) =>
              // information was recovered
              dataSpace.write(backupEntry.createDataEntry(), config.DataLeaseTime)
              Log.writeLog(LogDataRecovered(analyserId, backupInfoEntry.fromAct, activityTo, injectId))
          }
          recoverNextEntry()
      }
    }

    recoverNextEntry()
  }

  /**
    * Updates the table in the space
    */
  private def updateTableInSpace(config: ProtocolConfig, distributionTable: TableType): Unit = {
    {
      def prettyMap(entry: (String, Int)): (String, String, Int) = entry match {
        case (str, n) =>
          val (graphId, actId) = str.splitAt(str.indexOf(":"))
          (graphId.take(8), actId, n)
      }

      val prettyTable = distributionTable.toList.map(prettyMap).sortBy(_._2)
        .map { case (graphId, actId, n) => graphId + actId + " -> " + n }.mkString("(", ", ", ")")
      println(new SimpleDateFormat("HH:mm:ss").format(new Date()) + ": TABLE" + prettyTable)
    }

    signalSpace.take(templateTable, 0)
    val entryTable = templateTable.setPayload(AnalyserTable(distributionTable, config))
    signalSpace.write(entryTable, config.TableLeaseTime)
  }

  /**
    * Adds an entry to the tracker table
    */
  private def updateTrackerTable(config: ProtocolConfig,
                                 trackerTable: TrackerTableType,
                                 newEntry: NodeInfoType,
                                 currentTime: Long): TrackerTableType = {
    //updates the table with a new entry
    val expiryTime = config.NodeInfoExpiryTime + currentTime
    (newEntry, expiryTime) :: trackerTable.filterNot { case (nodeInfo, _) => nodeInfo.nodeId == newEntry.nodeId }
  }

  /**
    * Cleans the expired infos from the trackerTable
    */
  private def cleanExpiredTrackerTable(trackerTable: TrackerTableType, currentTime: Long): TrackerTableType = {
    trackerTable.filter { case (_, expiryTime) => expiryTime > currentTime }
  }

  private def updateDistributionTable(trackerTable: TrackerTableType,
                                      distributionTable: TableType,
                                      currentTime: Long): TableType = {
    //Cleans the table of dead or busy nodes:
    val groupedByActivity =
      trackerTable
        .groupBy { case (nodeInfo, _) => nodeInfo.activityId }
        .filter { case (actId, _) => actId == ProtocolConfig.UndefinedActId || distributionTable.contains(actId) }
    val countOfNodesByActivity: Map[String, Int] = groupedByActivity.mapValues(_.size)

    countOfNodesByActivity ++ {
      for {
        (id, _) <- distributionTable
        if countOfNodesByActivity.get(id).isEmpty
      } yield id -> 0
    }
  }

  implicit def mapToHashMap[A, B](map: Map[A, B]): HashMap[A, B] = {
    HashMap(map.toSeq: _*)
  }

  implicit def seqToHashMap[A, B](seq: Seq[(A, B)]): HashMap[A, B] = {
    HashMap(seq: _*)
  }

}
