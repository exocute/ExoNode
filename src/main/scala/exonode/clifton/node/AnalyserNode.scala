package exonode.clifton.node

import java.text.SimpleDateFormat
import java.util.Date

import exonode.clifton.Protocol._

import scala.collection.immutable.HashMap
import scala.language.implicitConversions
import scala.collection.JavaConverters._

/**
  * Created by #ScalaTeam on 05/01/2017.
  *
  * AnalyserNode is responsible for updating the space with a table of how many nodes
  * are processing some activity. Analyser Node it's one of the modes of what a clifton node
  * can be transformed.
  */
class AnalyserNode(nodeId: String, tab: TableType) {

  //the number of entries to be taken from space from defined time
  private val MAX_INFO_CALL = 20

  private val signalSpace = SpaceCache.getSignalSpace

  private val numAct = tab.size - 1

  //ID, ActID, expiryTime (in milliseconds)
  type TrackerEntry = (String, String, Long)

  private var trackerTable = Set[TrackerEntry]()
  private var lastExpiryUpdate = System.currentTimeMillis()
  private var receivedInfos = false

  val tmplInfo = new ExoEntry(INFO_MARKER, null)
  var tmplTable = new ExoEntry(TABLE_MARKER, null)


  //takes the first tabble from space and updates the Analyser_Act_ID to 1
  private var actDistributionTable: TableType = {
    for {
      (entryNo, _) <- tab
      if entryNo != ANALYSER_ACT_ID
    } yield (entryNo, 0)
  } + (ANALYSER_ACT_ID -> 1)


  def startAnalysing(): Unit = {

    var lastUpdateTime = System.currentTimeMillis()

    val bootMessage = s"Analyser node $nodeId started"
    println(bootMessage)
    Log.info(bootMessage)

    tmplTable.payload = actDistributionTable
    signalSpace.write(tmplTable, TABLE_LEASE_TIME)

    while (true) {
      //RECEIVE:
      //get a TrackerEntry from the signal space
      val infoEntries = signalSpace.takeMany(tmplInfo, MAX_INFO_CALL).asScala

      //insert new TrackEntry -> ex: analiser.updateTrackerTable(("ID6", "C", 1))
      if (infoEntries.nonEmpty) {
        for (entry <- infoEntries) {
          val info = entry.payload.asInstanceOf[TrackerEntry]
          updateTrackerTable(info)
        }
      } else {
        Thread.sleep(ANALYSER_SLEEP_TIME)
      }

      //SEND:
      if (System.currentTimeMillis() - lastUpdateTime >= TABLE_UPDATE_TIME) {
        if (receivedInfos) {
          updateActDistributionTable()
          receivedInfos = false
        }
        println(new SimpleDateFormat("HH:mm:ss").format(new Date()) + ": " + actDistributionTable)
        signalSpace.take(tmplTable, 0L)
        tmplTable.payload = actDistributionTable
        signalSpace.write(tmplTable, TABLE_LEASE_TIME)
        lastUpdateTime = System.currentTimeMillis()
      }
    }
  }


  def updateTrackerTable(newEntry: TrackerEntry): Unit = {
    //updates the table with a new entry
    trackerTable = trackerTable.filterNot { case (id, _, _) => id == newEntry._1 } + newEntry
    receivedInfos = true
  }

  /**
    * Cleans the expired info from table
    */
  def cleanExpiredTrackerTable(): Unit = {
    val currentTime = System.currentTimeMillis()
    val elapsedTime = currentTime - lastExpiryUpdate

    val newTable = trackerTable.map { case (id, actId, expiryTime) => (id, actId, expiryTime - elapsedTime) }

    //cleanedTable
    trackerTable = newTable.filter { case (_, _, expiryTime) => expiryTime >= 0 }
    lastExpiryUpdate = currentTime
  }

  def updateActDistributionTable(): Unit = {
    //Cleans the table of dead or busy nodes:
    cleanExpiredTrackerTable()

    val groupedByActivity = trackerTable.groupBy { case (_, actId, _) => actId }
    val countOfNodesByActivity: Map[String, Int] = groupedByActivity.mapValues(_.size).
      +((ANALYSER_ACT_ID, actDistributionTable.getOrElse(ANALYSER_ACT_ID, 0)))
    //    println( ( trackerTable.groupBy(_._2).mapValues(_.size) + ( ("@", actDistributionTable.getOrElse("@",0) ) ) ) )

    val newActDistributionTable = {
      for {
        (id, q) <- actDistributionTable
      } yield (id, countOfNodesByActivity.getOrElse(id, 0))
    }

    actDistributionTable = newActDistributionTable

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
