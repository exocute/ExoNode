package exonode.clifton.node.work

import java.text.SimpleDateFormat
import java.util.Date

import exonode.clifton.config.BackupConfig
import exonode.clifton.config.Protocol._
import exonode.clifton.node.Log.{ERROR, ND, WARN}
import exonode.clifton.node._
import exonode.clifton.node.entries.{BackupInfoEntry, ExoEntry, GraphEntry}
import exonode.clifton.signals.LoggingSignal

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
class AnalyserThread(analyserId: String)(implicit backupConfig: BackupConfig) extends Thread with BusyWorking with Analyser {

  //the number of entries to be taken from space from a defined time
  private val MAX_INFO_CALL = 20
  private val MAX_GRAPHS = 100

  private val signalSpace = SpaceCache.getSignalSpace
  private val dataSpace = SpaceCache.getDataSpace

  private type TrackerEntryType = (NodeInfoType, Long)
  private type TrackerTableType = List[TrackerEntryType]
  //((actID, injID)), time)
  private type TableBackupType = HashMap[(String, String, String), Long]

  private val templateInfo = ExoEntry[NodeInfoType](INFO_MARKER, null)
  private val templateTable = ExoEntry[TableType](TABLE_MARKER, null)
  private val templateGraph = GraphEntry(null, null)
  private val templateBackup = BackupInfoEntry(null, null, null, null)

  override def threadIsBusy = true

  private def reloadGraphs(distributionTable: TableType): TableType = {
    //    graphsChanged = false
    val graphs = signalSpace.readMany(templateGraph, MAX_GRAPHS)
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

  private var cancel = false

  override def cancelThread(): Unit = cancel = true

  override def run(): Unit = {
    try {
      val trackerTable: TrackerTableType = Nil
      val initialTable = reloadGraphs(EMPTY_TABLE)
      val entryTable = templateTable.setPayload(initialTable)
      signalSpace.take(templateTable, ENTRY_READ_TIME)
      signalSpace.write(entryTable, TABLE_LEASE_TIME)
      val currentTime = System.currentTimeMillis()
      val backupsTable: TableBackupType = new HashMap[(String, String, String), Long]()
      readInfosFromSpace(currentTime, currentTime, currentTime, trackerTable, initialTable, backupsTable)
    } catch {
      case _: Throwable =>
    }
  }

  private def updateBackupTable(backupTable: TableBackupType, info: NodeInfoType): TableBackupType = {
    val (_, actId, injectId, orderId) = info
    val backupEntry = (actId, injectId, orderId)
    if (backupTable.contains(backupEntry)) {
      backupTable.updated(backupEntry, System.currentTimeMillis() + backupConfig.BACKUP_TIMEOUT_TIME)
    } else backupTable
  }

  private def updateBackup(table: TableBackupType, info: BackupInfoEntry): TableBackupType = {
    val infoBackup = (info.toAct, info.injectId, info.orderId)
    if (table.contains(infoBackup))
      table
    else
      table.updated(infoBackup, System.currentTimeMillis() + backupConfig.BACKUP_TIMEOUT_TIME)
  }

  private def convertToData(activityTo: String, injectId: String, orderId: String): BackupInfoEntry = {
    BackupInfoEntry(activityTo, null, injectId, orderId)
  }

  private def recoveryData(activityTo: String, injectId: String, orderId: String): Unit = {
    val backupInfoTemplate = convertToData(activityTo, injectId, orderId)

    def recoverNextEntry(): Unit = {
      dataSpace.take(backupInfoTemplate, ENTRY_READ_TIME) match {
        case None => // No backups left
        case Some(backupInfoEntry) =>
          val backupEntryTemplate = backupInfoEntry.createTemplate()
          dataSpace.take(backupEntryTemplate, ENTRY_READ_TIME) match {
            case None =>
              // information was lost
              Log.receiveLog(LoggingSignal(LOGCODE_INFORMATION_LOST, ERROR, analyserId, ND, ND, ND, injectId, s"Data with inject id $injectId for activity $activityTo wasn't recoverable", 0))
            case Some(backupEntry) =>
              dataSpace.write(backupEntry.createDataEntry(), DATA_LEASE_TIME)
              Log.receiveLog(LoggingSignal(LOGCODE_DATA_RECOVERED, WARN, analyserId, ND, ND, ND, injectId, s"Data with inject id $injectId for activity $activityTo was recovered successfully", 0))
          }
          recoverNextEntry()
      }
    }

    recoverNextEntry()
  }

  @tailrec
  private def readInfosFromSpace(lastUpdateTime: Long, backupsTime: Long, graphsChangedTime: Long,
                                 trackerTable: TrackerTableType, originalDistributionTable: TableType,
                                 backupsTable: TableBackupType): Unit = {
    if (!cancel) {
      //RECEIVE:
      //get many NodeInfoType from the signal space

      val infoEntries = signalSpace.takeMany(templateInfo, MAX_INFO_CALL)

      val currentTime = System.currentTimeMillis()

      val (updatedTrackerTable, newBackupTable) =
        if (infoEntries.nonEmpty) {
          val nodeInfos = infoEntries.map(_.payload).filterNot(_._1 == analyserId)
          val (notProcessing, processing) = nodeInfos.partition(_._3 == NOT_PROCESSING_MARKER)
          val trackUpdateTable = notProcessing.foldLeft(trackerTable)((tracker, info) => updateTrackerTable(tracker, info, currentTime))
          val backupUpdateTable = processing.foldLeft(backupsTable)((backupTable, info) => updateBackupTable(backupTable, info))
          (trackUpdateTable, backupUpdateTable)
        } else {
          (trackerTable, backupsTable)
        }

      val (updatedDistributionTable, updatedGraphsChangedTime) = {
        val distTable = updateDistributionTable(trackerTable, originalDistributionTable, currentTime)
        if (currentTime - graphsChangedTime > ANALYSER_CHECK_GRAPHS_TIME)
          (reloadGraphs(distTable), currentTime)
        else
          (distTable, graphsChangedTime)
      }

      // Update backup information
      val (updatedBackupsTable, newBackupsTime) =
        if (currentTime - backupsTime > backupConfig.ANALYSER_CHECK_BACKUP_INFO) {
          val maxNodes = originalDistributionTable.size
          val backupInfos = dataSpace.readMany(templateBackup, updatedTrackerTable.size * maxNodes)
          val notFilterBackupTable = backupInfos.foldLeft(newBackupTable)((table, info) => updateBackup(table, info))
          val (expiredBackups, backupTable) = notFilterBackupTable.partition(_._2 < currentTime)
          for (((activityTo, injectId, orderId), _) <- expiredBackups)
            recoveryData(activityTo, injectId, orderId)
          (backupTable, currentTime)
        } else
          (newBackupTable, backupsTime)

      // Send updated distribution table to space
      val (newUpdateTime, finalTrackerTable, finalDistributionTable) =
        if (currentTime - lastUpdateTime >= TABLE_UPDATE_TIME) {
          val cleanTrackerTable = cleanExpiredTrackerTable(updatedTrackerTable, currentTime)
          val newDistributionTable = updateDistributionTable(cleanTrackerTable, updatedDistributionTable, currentTime)
          updateTableInSpace(newDistributionTable)
          (currentTime, cleanTrackerTable, newDistributionTable)
        } else {
          (lastUpdateTime, updatedTrackerTable, updatedDistributionTable)
        }

      if (infoEntries.size < MAX_INFO_CALL)
        Thread.sleep(ANALYSER_SLEEP_TIME)

      readInfosFromSpace(newUpdateTime, newBackupsTime, updatedGraphsChangedTime,
        finalTrackerTable, finalDistributionTable, updatedBackupsTable)
    }
  }

  private def updateTableInSpace(distributionTable: TableType): Unit = {
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

    signalSpace.take(templateTable, ENTRY_READ_TIME)
    val entryTable = templateTable.setPayload(distributionTable)
    signalSpace.write(entryTable, TABLE_LEASE_TIME)
  }

  private def updateTrackerTable(trackerTable: TrackerTableType, newEntry: NodeInfoType, currentTime: Long): TrackerTableType = {
    //updates the table with a new entry
    val expiryTime = NODE_INFO_EXPIRY_TIME + currentTime
    (newEntry, expiryTime) :: trackerTable.filterNot { case ((id, _, _, _), _) => id == newEntry._1 }
  }

  /**
    * Cleans the expired info from table
    */
  private def cleanExpiredTrackerTable(trackerTable: TrackerTableType, currentTime: Long): TrackerTableType = {
    trackerTable.filter { case (_, expiryTime) => expiryTime > currentTime }
  }

  private def updateDistributionTable(trackerTable: TrackerTableType, distributionTable: TableType, currentTime: Long): TableType = {
    //Cleans the table of dead or busy nodes:
    val groupedByActivity =
      trackerTable
        .groupBy { case ((_, actId, _, _), _) => actId }
        .filter { case (actId, _) => actId == UNDEFINED_ACT_ID || distributionTable.contains(actId) }
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
