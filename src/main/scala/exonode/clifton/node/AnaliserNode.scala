package exonode.clifton.node

import exonode.clifton.Protocol._

import scala.collection.immutable.HashMap

/**
  * Created by #ScalaTeam on 05/01/2017.
  */
class AnaliserNode(actNames: List[String], graphID: String) extends Thread {

  private val signalSpace = SpaceCache.getSignalSpace
  private val dataSpace = SpaceCache.getDataSpace

  private val numAct = actNames.length

  val startingNQ: (Double, Double) = (1.0 / numAct, 0)
  val startingAnaliserNQ: (Double, Double) = (2, 0)

  //ID, ActID, expiryTime (in seconds)
  type TrackerEntry = (String, String, Long)

  private var trackerTable = Set[TrackerEntry]()
  private var lastExpiryUpdate = System.currentTimeMillis()

  val tmplInfo = new ExoEntry(INFO_MARKER, null)
  var tmplTable = new ExoEntry(TABLE_MARKER, null)

  override def run(): Unit = {

    var boot = System.currentTimeMillis()

    println("Analiser Started")

    while (true) {
      //RECEIVE:
      //get a TrackerEntry from the signal space
      val entry = signalSpace.take(tmplInfo, 0L)

      //insert new TrackEntry -> ex: analiser.updateTrackerTable(("ID6", "C", 1))
      if (entry != null) {
        val info = entry.payload.asInstanceOf[TrackerEntry]
        updateTrackerTable(info)
      }
      else {
        Thread.sleep(1000)
      }
      //update distribution -> analiser.actDistributionTable

      //SEND:
      if (System.currentTimeMillis() - boot >= TABLE_UPDATE_TIME) {
        updateActDistributionTable()
        signalSpace.take(tmplTable, 0L)
        tmplTable.payload = actDistributionTable
        signalSpace.write(tmplTable, TABLE_LEASE_TIME)
        boot = System.currentTimeMillis()
      }

    }
  }

  private var actDistributionTable: HashMap[String, (Double, Double)] = HashMap(
    {
      for {
        entryNo <- 0 until numAct
      } yield (actNames(entryNo), startingNQ)
    } :+ (ANALISER_ID, startingAnaliserNQ): _*
  )

  def updateTrackerTable(newEntry: TrackerEntry): Unit = {
    //updates the table with a new entry
    trackerTable = trackerTable.filterNot { case (id, _, _) => id == newEntry._1 } + newEntry
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
    val countOfNodesByActivity: Map[String, Double] = groupedByActivity.mapValues(_.size)
    val totalNodes: Double =
      if (trackerTable.isEmpty) 1
      else trackerTable.size


    val newActDistributionTable = {
      for {
        (id, (n, q)) <- actDistributionTable
      } yield (id, (n, countOfNodesByActivity.getOrElse(id, 0.0) / totalNodes))
    }

    actDistributionTable = newActDistributionTable
  }

}
