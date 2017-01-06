package exonode.clifton.node

import scala.collection.immutable.HashMap

/**
  * Created by #ScalaTeam on 05/01/2017.
  */
class AnaliserNode(actNames: List[String], graphID: String) extends Thread {

  val signalSpace = SpaceCache.getSignalSpace
  val dataSpace = SpaceCache.getDataSpace

  private val numAct = actNames.length

  val startingNQ: (Double, Double) = (1.0 / numAct, 0)
  val startingAnaliserNQ: (Double, Double) = (2, 0)

  //ID, ActID, expiryTime (in seconds)
  type TrackerEntry = (String, String, Long)

  private var trackerTable = Set[TrackerEntry]()
  private var lastExpiryUpdate = System.currentTimeMillis()

  val tmplInfo = new ExoEntry("INFO", null)
  var tmplTable = new ExoEntry("TABLE", null)


  val MAX_TIME = 4 * 1000

  val WRITE_TIME: Long = 60 * 1000

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
      if (System.currentTimeMillis() - boot >= MAX_TIME) {
        updateActDistributionTable()
        signalSpace.take(tmplTable, 0L)
        tmplTable.payload = actDistributionTable
        signalSpace.write(tmplTable, WRITE_TIME)
        boot = System.currentTimeMillis()
      }

    }
  }

  private var actDistributionTable: HashMap[String, (Double, Double)] = HashMap(
    {
      for {
        entryNo <- 0 to (numAct - 1)
      } yield (actNames(entryNo), startingNQ)
    } :+ ("@", startingAnaliserNQ): _*
  )


  def updateTrackerTable(newEntry: TrackerEntry): Unit = {
    val cleanUpTable = trackerTable
    //FIXME sub the upper line by the one bellow when, during execution,
    // we start considering node "still alive" ping expiryTimes
    //val cleanUpTable = cleanExpiredTrackerTable(trackerTable)
    trackerTable = cleanUpTable.filterNot(x => x._1 == newEntry._1) + newEntry
  }


  def cleanExpiredTrackerTable(currentTable: Set[TrackerEntry]): Unit = {
    val currentTime = System.currentTimeMillis()
    val elapsedTime = currentTime - lastExpiryUpdate

    val newTable = currentTable.map(x => (x._1, x._2, x._3 - elapsedTime / 1000))
    //cleanedTable
    trackerTable = newTable.filter(x => x._3 < 0)
    lastExpiryUpdate = currentTime
  }


  def updateActDistributionTable(): Unit = {
    val groupedByActivity = trackerTable.groupBy(x => x._2)
    val countOfNodesByActivity: Map[String, Double] = groupedByActivity.mapValues(_.size)
    val totalNodes: Double =
      if (trackerTable.size == 0) 1
      else trackerTable.size


    val newActDistributionTable = {
      for {
        act <- actDistributionTable
      } yield (act._1, (act._2._1, countOfNodesByActivity.getOrElse(act._1, 0.0) / totalNodes))
    }

    actDistributionTable = newActDistributionTable
  }

}
