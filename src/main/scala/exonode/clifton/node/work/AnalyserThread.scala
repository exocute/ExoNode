package exonode.clifton.node.work

import java.text.SimpleDateFormat
import java.util.Date

import com.zink.fly.NotifyHandler
import com.zink.fly.example.NotifyLeaseRenewer
import exonode.clifton.Protocol._
import exonode.clifton.node._

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

  private type TrackerEntryType = (NodeInfoType, Long)
  private type TrackerTableType = List[TrackerEntryType]

  private val templateInfo = ExoEntry(INFO_MARKER, null)
  private val templateTable = ExoEntry(TABLE_MARKER, null)
  private val templateGraph = ExoEntry(GRAPH_MARKER, null)

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

  object GraphChangeNotifier extends NotifyHandler {
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
      val lastUpdateTime = System.currentTimeMillis()
      readInfosFromSpace(lastUpdateTime, receivedInfos = false, trackerTable, initialTable)
    } catch {
      case _: InterruptedException =>
    }
  }

  @tailrec
  private def readInfosFromSpace(lastUpdateTime: Long, receivedInfos: Boolean,
                                 trackerTable: TrackerTableType, originalDistributionTable: TableType) {
    //RECEIVE:
    //get many NodeInfoType from the signal space
    val infoEntries = signalSpace.takeMany(templateInfo, MAX_INFO_CALL)

    val currentTime = System.currentTimeMillis()

    val newTrackerTable =
      if (infoEntries.nonEmpty) {
        val nodeInfos = infoEntries.map(_.payload.asInstanceOf[NodeInfoType])
        nodeInfos.foldLeft(trackerTable)((tracker, info) => updateTrackerTable(tracker, info, currentTime))
      } else {
        Thread.sleep(ANALYSER_SLEEP_TIME)
        trackerTable
      }

    val updatedReceivedInfos = receivedInfos || infoEntries.nonEmpty

    val distributionTable =
      if (graphsChanged)
        reloadGraphs(originalDistributionTable)
      else
        originalDistributionTable

    //SEND:
    if (currentTime - lastUpdateTime >= TABLE_UPDATE_TIME) {
      if (updatedReceivedInfos) {
        val cleanTrackerTable = cleanExpiredTrackerTable(newTrackerTable, currentTime)
        val newDistributionTable = updateDistributionTable(cleanTrackerTable, distributionTable, currentTime)
        updateTableInSpace(newDistributionTable)
        readInfosFromSpace(currentTime, updatedReceivedInfos, newTrackerTable, newDistributionTable)
      } else {
        updateTableInSpace(distributionTable)
        readInfosFromSpace(currentTime, updatedReceivedInfos, newTrackerTable, distributionTable)
      }
    } else {
      readInfosFromSpace(lastUpdateTime, updatedReceivedInfos, newTrackerTable, distributionTable)
    }
  }

  def updateTableInSpace(distributionTable: TableType): Unit = {
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

  def updateTrackerTable(trackerTable: TrackerTableType, newEntry: NodeInfoType, currentTime: Long): TrackerTableType = {
    //updates the table with a new entry
    val expiryTime = NODE_INFO_EXPIRY_TIME + currentTime
    (newEntry, expiryTime) :: trackerTable.filterNot { case ((id, _), _) => id == newEntry._1 }
  }

  /**
    * Cleans the expired info from table
    */
  def cleanExpiredTrackerTable(trackerTable: TrackerTableType, currentTime: Long): TrackerTableType = {
    trackerTable.filter { case (_, expiryTime) => expiryTime > currentTime }
  }

  def updateDistributionTable(trackerTable: TrackerTableType, distributionTable: TableType, currentTime: Long): TableType = {
    //Cleans the table of dead or busy nodes:
    val groupedByActivity = trackerTable.groupBy { case ((_, actId), _) => actId }
    val countOfNodesByActivity: Map[String, Int] = groupedByActivity.mapValues(_.size).
      +((ANALYSER_ACT_ID, distributionTable.getOrElse(ANALYSER_ACT_ID, 0)))

    val newActDistributionTable = countOfNodesByActivity ++ {
      for {
        (id, _) <- distributionTable
        if countOfNodesByActivity.get(id).isEmpty
      } yield id -> 0
    }

    newActDistributionTable

    //    actDistributionTable = trackerTable.groupBy(_._2).mapValues(_.size) +
    //      ((ANALYSER_ACT_ID, actDistributionTable.getOrElse(ANALYSER_ACT_ID, 0)))
  }

  implicit def mapToHashMap[A, B](map: Map[A, B]): HashMap[A, B] = {
    HashMap(map.toSeq: _*)
  }

  implicit def seqToHashMap[A, B](seq: Seq[(A, B)]): HashMap[A, B] = {
    HashMap(seq: _*)
  }

}
