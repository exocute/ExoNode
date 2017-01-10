package exonode.clifton.node

import exonode.clifton.Protocol._

import scala.collection.immutable.HashMap

/**
  * Created by #ScalaTeam on 05/01/2017.
  */
class AnaliserNode(actNames: List[String], graphId: String) extends Thread {

  private val signalSpace = SpaceCache.getSignalSpace
  private val dataSpace = SpaceCache.getDataSpace

  private val numAct = actNames.length

  //ID, ActID, expiryTime (in milliseconds)
  type TrackerEntry = (String, String, Long)

  private var trackerTable = Set[TrackerEntry]()
  private var lastExpiryUpdate = System.currentTimeMillis()
  private var receivedInfos = false

  val tmplInfo = new ExoEntry(INFO_MARKER, null)
  var tmplTable = new ExoEntry(TABLE_MARKER, null)

  override def run(): Unit = {

    var lastUpdateTime = System.currentTimeMillis()

    println("Analiser Started")

    while (true) {
      //RECEIVE:
      //get a TrackerEntry from the signal space
      val entry = signalSpace.take(tmplInfo, ENTRY_READ_TIME)

      //insert new TrackEntry -> ex: analiser.updateTrackerTable(("ID6", "C", 1))
      if (entry != null) {
        val info = entry.payload.asInstanceOf[TrackerEntry]
        updateTrackerTable(info)
      } else {
        Thread.sleep(ANALISER_SLEEP_TIME)
      }
      //update distribution -> analiser.actDistributionTable

      //SEND:
      if (System.currentTimeMillis() - lastUpdateTime >= TABLE_UPDATE_TIME) {
        if (receivedInfos) {
          updateActDistributionTable()
          receivedInfos = false
        }
        println(actDistributionTable)
        signalSpace.take(tmplTable, 0L)
        tmplTable.payload = actDistributionTable
        signalSpace.write(tmplTable, TABLE_LEASE_TIME)
        lastUpdateTime = System.currentTimeMillis()
      }
    }
  }

  private var actDistributionTable: TableType = {
    for {
      entryNo <- 0 until numAct
    } yield (actNames(entryNo), 0)
  } :+ (ANALISER_ACT_ID, 0)


  def updateTrackerTable(newEntry: TrackerEntry): Unit = {
    //updates the table with a new entry
    trackerTable = trackerTable.filterNot { case (id, _, _) => id == newEntry._1 } + newEntry
    receivedInfos = true
  }

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
    val countOfNodesByActivity: Map[String, Int] = groupedByActivity.mapValues(_.size)
//    println( ( trackerTable.groupBy(_._2).mapValues(_.size) + ( ("@", actDistributionTable.getOrElse("@",0) ) ) ) )

    val newActDistributionTable = {
      for {
        (id, q) <- actDistributionTable
      } yield (id, countOfNodesByActivity.getOrElse(id, 0) )
    }

    actDistributionTable = newActDistributionTable

//    actDistributionTable = trackerTable.groupBy(_._2).mapValues(_.size) +
//      ((ANALISER_ACT_ID, actDistributionTable.getOrElse(ANALISER_ACT_ID, 0)))
  }

  implicit def mapToHashMap[A, B](map: Map[A, B]): HashMap[A, B] = {
    HashMap(map.toSeq: _*)
  }

  implicit def seqToHashMap[A, B](seq: Seq[(A, B)]): HashMap[A, B] = {
    HashMap(seq: _*)
  }

}
