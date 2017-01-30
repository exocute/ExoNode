package exonode.clifton.node.work

import java.text.SimpleDateFormat
import java.util.Date

import com.zink.fly.NotifyHandler
import exonode.clifton.Protocol._
import exonode.clifton.node._
import exonode.clifton.node.entries.{BackupInfoEntry, ExoEntry}

import scala.annotation.tailrec
import scala.collection.immutable.HashMap
import scala.language.implicitConversions

/**
  * Created by #ScalaTeam on 05/01/2017.
  *
  * AnalyserNode is responsible for updating the space with a table of how many nodes
  * are processing some activity. Analyser Node it's one of the modes of what a clifton node
  * can be transformed.
  */
class AnalyserThread(nodeId: String, initialTable: TableType) extends Thread with BusyWorking {

  //the number of entries to be taken from space from a defined time
  private val MAX_INFO_CALL = 20
  private val MAX_GRAPHS = 100

  private val signalSpace = SpaceCache.getSignalSpace
  private val dataSpace = SpaceCache.getDataSpace

  private type TrackerEntryType = (NodeInfoType, Long)
  private type TrackerTableType = List[TrackerEntryType]
  //((actID, injID)), time)
  private type TableBackupType = HashMap[(String, String), Long]


  private val templateInfo = ExoEntry(INFO_MARKER, null)
  private val templateTable = ExoEntry(TABLE_MARKER, null)
  private val templateGraph = ExoEntry(GRAPH_MARKER, null)
  private val templateBackup = BackupInfoEntry(null, null, null)

  //takes the table from the space and updates the Analyser_Act_ID to 1
  private def createInitialTable(): TableType = {
    for {
      (entryNo, _) <- initialTable
      if entryNo != ANALYSER_ACT_ID
    } yield entryNo -> 0
  } + (ANALYSER_ACT_ID -> 1)


  override def threadIsBusy = true

  private def reloadGraphs(distributionTable: TableType): TableType = {
    val graphs = signalSpace.readMany(templateGraph, MAX_GRAPHS)
    distributionTable ++ (for {
      graph <- graphs
      (graphId, activities) = graph.payload.asInstanceOf[GraphEntryType]
      activityId <- activities
      fullId = graphId + ":" + activityId
      if !distributionTable.contains(fullId)
    } yield fullId -> 0).toSeq
  }

  private var graphsChanged: Boolean = false

  private object GraphChangeNotifier extends NotifyHandler {
    override def templateMatched(): Unit = graphsChanged = true
  }

  override def run(): Unit = {
    try {
      new NotifyWriteRenewer(signalSpace, templateGraph, GraphChangeNotifier, NOTIFY_GRAPHS_ANALYSER_TIME)
      new NotifyTakeRenewer(signalSpace, templateGraph, GraphChangeNotifier, NOTIFY_GRAPHS_ANALYSER_TIME)

      val trackerTable: TrackerTableType = Nil
      val initialTable = reloadGraphs(createInitialTable())

      val entryTable = templateTable.setPayload(initialTable)
      signalSpace.write(entryTable, TABLE_LEASE_TIME)
      val currentTime = System.currentTimeMillis()
      val backupsTable: TableBackupType = new HashMap[(String, String), Long]()
      readInfosFromSpace(currentTime, currentTime, trackerTable, initialTable, backupsTable)
    } catch {
      case _: InterruptedException =>
    }
  }

  private def updateBackupTable(backupTable: TableBackupType, info: NodeInfoType): TableBackupType = {
    val (_, actId, injId) = info
    val backupEntry = (actId, injId)
    if (backupTable.contains(backupEntry)) {
      backupTable.updated(backupEntry, System.currentTimeMillis() + BACKUP_MAX_LIVE_TIME)
    } else backupTable
  }


  private def updateBackup(table: TableBackupType, info: BackupInfoEntry): TableBackupType = {
    val infoBackup = (info.toAct, info.injectId)
    if (table.contains(infoBackup))
      table
    else
      table.updated(infoBackup, System.currentTimeMillis() + BACKUP_MAX_LIVE_TIME)
  }

  private def convertToData(backup: ((String, String), Long)): BackupInfoEntry = {
    backup match {
      case ((to, injId), _) => BackupInfoEntry(to, null, injId)
    }
  }

  private def recoveryData(backup: ((String, String), Long)): Unit = {
    val backupInfoTemplate = convertToData(backup)

    def recoverNextEntry(): Unit = {
      dataSpace.take(backupInfoTemplate, ENTRY_READ_TIME) match {
        case None => // No backups left
        case Some(backupInfoEntry) =>
          val backupEntryTemplate = backupInfoEntry.createTemplate()
          dataSpace.take(backupEntryTemplate, ENTRY_READ_TIME) match {
            case None =>
              // information was lost
              Log.error(s"$nodeId($ANALYSER_ACT_ID)", s"Data with inject id ${backupEntryTemplate.injectId} wasn't recoverable")
            case Some(backupEntry) =>
              dataSpace.write(backupEntry.createDataEntry(), DATA_LEASE_TIME)
          }
          recoverNextEntry()
      }
    }
    recoverNextEntry()
  }

  @tailrec
  private def readInfosFromSpace(lastUpdateTime: Long, backupsTime: Long, trackerTable: TrackerTableType,
                                 originalDistributionTable: TableType, backupsTable: TableBackupType) {
    //RECEIVE:
    //get many NodeInfoType from the signal space

    val infoEntries = signalSpace.takeMany(templateInfo, MAX_INFO_CALL)

    val currentTime = System.currentTimeMillis()

    val (updatedTrackerTable, newBackupTable) =
      if (infoEntries.nonEmpty) {
        val nodeInfos = infoEntries.map(_.payload.asInstanceOf[NodeInfoType])
        val (notProcessing, processing) = nodeInfos.partition(_._3 == NOT_PROCESSING_MARKER)
        val trackUpdateTable = notProcessing.foldLeft(trackerTable)((tracker, info) => updateTrackerTable(tracker, info, currentTime))
        val backupUpdateTable = processing.foldLeft(backupsTable)((backupTable, info) => updateBackupTable(backupTable, info))
        (trackUpdateTable, backupUpdateTable)
      } else {
        Thread.sleep(ANALYSER_SLEEP_TIME)
        (trackerTable, backupsTable)
      }

    val updatedDistributionTable = {
      val distTable = updateDistributionTable(trackerTable, originalDistributionTable, currentTime)
      if (graphsChanged)
        reloadGraphs(distTable)
      else
        distTable
    }

    // Update backup information
    val (updatedBackupsTable, newBackupsTime) =
      if (currentTime - backupsTime > ANALYSER_CHECK_BACKUP_INFO) {
        val maxNodes = originalDistributionTable.size
        val backupInfos = dataSpace.readMany(templateBackup, updatedTrackerTable.size * maxNodes)
        val notFilterBackupTable = backupInfos.foldLeft(newBackupTable)((table, info) => updateBackup(table, info))
        val (expiredBackups, backupTable) = notFilterBackupTable.partition(_._2 < currentTime)
        for (backup <- expiredBackups)
          recoveryData(backup)
        (backupTable, currentTime)
      } else
        (newBackupTable, backupsTime)

    // Send updated distribution table to space
    if (currentTime - lastUpdateTime >= TABLE_UPDATE_TIME) {
      val cleanTrackerTable = cleanExpiredTrackerTable(updatedTrackerTable, currentTime)
      val newDistributionTable = updateDistributionTable(cleanTrackerTable, updatedDistributionTable, currentTime)
      updateTableInSpace(newDistributionTable)
      readInfosFromSpace(currentTime, newBackupsTime, cleanTrackerTable, newDistributionTable, updatedBackupsTable)
    } else {
      readInfosFromSpace(lastUpdateTime, newBackupsTime, updatedTrackerTable, updatedDistributionTable, updatedBackupsTable)
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
      println(new SimpleDateFormat("HH:mm:ss").format(new Date()) + ": " + prettyTable)
    }

    signalSpace.take(templateTable, ENTRY_READ_TIME)
    val entryTable = templateTable.setPayload(distributionTable)
    signalSpace.write(entryTable, TABLE_LEASE_TIME)
  }

  private def updateTrackerTable(trackerTable: TrackerTableType, newEntry: NodeInfoType, currentTime: Long): TrackerTableType = {
    //updates the table with a new entry
    val expiryTime = NODE_INFO_EXPIRY_TIME + currentTime
    (newEntry, expiryTime) :: trackerTable.filterNot { case ((id, _, _), _) => id == newEntry._1 }
  }

  /**
    * Cleans the expired info from table
    */
  private def cleanExpiredTrackerTable(trackerTable: TrackerTableType, currentTime: Long): TrackerTableType = {
    trackerTable.filter { case (_, expiryTime) => expiryTime > currentTime }
  }

  private def updateDistributionTable(trackerTable: TrackerTableType, distributionTable: TableType, currentTime: Long): TableType = {
    //Cleans the table of dead or busy nodes:
    val groupedByActivity = trackerTable.groupBy { case ((_, actId, _), _) => actId }
    val countOfNodesByActivity: Map[String, Int] = groupedByActivity.mapValues(_.size).
      +((ANALYSER_ACT_ID, distributionTable.getOrElse(ANALYSER_ACT_ID, 0)))

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
