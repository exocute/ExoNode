package exonode.clifton.node

import java.io.Serializable
import java.util.UUID

import exonode.clifton.signals.{ActivitySignal, DataSignal}
import exonode.exocuteCommon.activity.Activity

import scala.collection.immutable.HashMap
import scala.util.Random

/**
  * Created by #ScalaTeam on 05-01-2017.
  */
class CliftonNode extends Thread {

  private val nodeId: String = UUID.randomUUID().toString

  private val signalSpace = SpaceCache.getSignalSpace
  private val dataSpace = SpaceCache.getDataSpace

  private val TIME_UPDATE_ACT = 5 * 60 * 1000
  private val MIN_TIME_BEFORE_CHECK = 5 * 1000
  private val MIN_SLEEP_TIME = 500
  private val MAX_SLEEP_TIME = 3 * 1000
  private val MAX_TIME_DATA = 5 * 60 * 1000

  def getRandomActivity(table: Map[String, (Double, Double)]): String = {
    val rnd = Random.nextDouble()
    val list = table.toList.filterNot(_._1 == "@")
    var acc = 0.0
    for ((s, (n, _)) <- list) {
      if (acc + n > rnd)
        return s
      else
        acc += n
    }
    list.last._1
  }

  override def run(): Unit = {

    //val bootTime = System.currentTimeMillis()

    //current worker definitions
    var worker: Option[(Activity, String, ActivitySignal)] = None

    //templates to search in space
    val templateAct = new ExoEntry("", null)
    val templateTable = new ExoEntry("TABLE", null)
    val templateData = new ExoEntry("", null)
    val templaceUpdateAct = new ExoEntry("INFO", null) // (idNode: String, idAct: String, valid: Long)

    //times initializer
    var idleTime = System.currentTimeMillis()
    var runningSince = 0L
    var sleepTime = MIN_SLEEP_TIME
    var checkTime = System.currentTimeMillis()

    while (true) {
      worker match {
        // worker not defined yet
        case None =>
          val tableEntry = signalSpace.read(templateTable, 0)
          if (tableEntry != null) {
            sleepTime = MIN_SLEEP_TIME
            val table = tableEntry.payload.asInstanceOf[HashMap[String, (Double, Double)]]
            val act = getRandomActivity(table)
            setActivity(act)
          } else {
            // if nothing was found, it will sleep for a while
            Thread.sleep(sleepTime)
            sleepTime = math.min(sleepTime + 1000, MAX_SLEEP_TIME)
          }

        //worker is defined
        case Some((activity, actId, activitySignal)) =>

          //get something to process
          val entry = dataSpace.take(templateData, 0L)
          if (entry != null) {
            //if something was found
            val dataSig = entry.payload.asInstanceOf[DataSignal]
            idleTime = 0
            sleepTime = MIN_SLEEP_TIME
            runningSince = System.currentTimeMillis()
            val result = activity.process(dataSig.res, activitySignal.params)
            println(actId + "- Processed " + dataSig.res + " --> " + result)
            insertNewResult(result, activitySignal, actId, dataSig.injectID)
          } else {
            // if nothing was found, it will sleep for a while
            Thread.sleep(sleepTime)
            sleepTime = math.min(sleepTime * 2, MAX_SLEEP_TIME)
          }

          //update space with current function
          signalSpace.write(templaceUpdateAct, TIME_UPDATE_ACT)

          //checks if its need to change mode
          transformNode(actId)
      }
    }

    def transformNode(actId: String) = {
      val nowTime = System.currentTimeMillis()
      if (nowTime - checkTime > MIN_TIME_BEFORE_CHECK) {
        val tableEntry = signalSpace.read(templateTable, 0)
        if (tableEntry != null) {
          val table = tableEntry.payload.asInstanceOf[HashMap[String, (Double, Double)]]
          val (n, q) = table(actId)
          //checks if its need to update function
          if (q > n && Random.nextDouble() < (q - n) / q) {
            //should i transform
            val newAct = getRandomActivity(table)
            if (newAct != actId) {
              setActivity(newAct)
            }
          }
        }
        checkTime = System.currentTimeMillis()
      }
    }

    def setActivity(activityId: String): Unit = {
      templateAct.marker = activityId
      val entry = signalSpace.read(templateAct, 0L)
      if (entry == null) {
        Log.error("Unknown Activity: " + activityId)
      } else entry.payload match {
        case activitySignal: ActivitySignal =>
          ActivityCache.getActivity(activitySignal.name) match {
            case Some(activity) =>
              templaceUpdateAct.payload = (nodeId, activityId, 0L)
              templateData.marker = activityId
              worker = Some(activity, activityId, activitySignal)
              (activity, activitySignal)
            case None => Log.error("Activity not found in JarSpace: " + activitySignal.name)
          }
      }
      //      }
    }

    def insertNewResult(result: Serializable, actSig: ActivitySignal, actId: String, injId: String) = {
      val to = actSig.outMarkers.head
      val tmplInsert = new ExoEntry(to, new DataSignal(to, actId, result, injId))
      dataSpace.write(tmplInsert, MAX_TIME_DATA)
    }

  }

}
