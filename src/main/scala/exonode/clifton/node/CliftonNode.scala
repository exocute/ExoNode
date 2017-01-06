package exonode.clifton.node

import java.io.Serializable
import java.util.UUID

import exonode.clifton.Protocol._
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

  def getRandomActivity(table: Map[String, (Double, Double)]): String = {
    val rnd = Random.nextDouble()
    val list = table.toList.filterNot(_._1 == "@")

    def find(l: List[(String, (Double, Double))], acc: Double): String = {
      l match {
        case Nil => list.last._1
        case (s, (n, _)) :: tail =>
          if (acc + n > rnd)
            s
          else
            find(tail, acc + n)
      }
    }

    find(list, 0.0)
  }

  override def run(): Unit = {

    //val bootTime = System.currentTimeMillis()

    //current worker definitions
    var worker: Option[(Activity, String, ActivitySignal)] = None

    //templates to search in space
    val templateAct = new ExoEntry("", null)
    val templateTable = new ExoEntry(TABLE_MARKER, null)
    val templateData = new ExoEntry("", null)
    val templaceUpdateAct = new ExoEntry(INFO_MARKER, null) // (idNode: String, idAct: String, valid: Long)

    //times initializer
    var idleTime = System.currentTimeMillis()
    var runningSince = 0L
    var sleepTime = NODE_MIN_SLEEP_TIME
    var checkTime = System.currentTimeMillis()

    while (true) {
      worker match {
        // worker not defined yet
        case None =>
          val tableEntry = signalSpace.read(templateTable, ENTRY_READ_TIME)
          if (tableEntry != null) {
            sleepTime = NODE_MIN_SLEEP_TIME
            val table = tableEntry.payload.asInstanceOf[HashMap[String, (Double, Double)]]
            val act = getRandomActivity(table)
            setActivity(act)
          } else {
            // if nothing was found, it will sleep for a while
            Thread.sleep(sleepTime)
            sleepTime = math.min(sleepTime * 2, NODE_MAX_SLEEP_TIME)
          }

        //worker is defined
        case Some((activity, actId, activitySignal)) =>

          //get something to process
          val entry = dataSpace.take(templateData, ENTRY_READ_TIME)
          if (entry != null) {
            //if something was found
            val dataSig = entry.payload.asInstanceOf[DataSignal]
            idleTime = 0
            sleepTime = NODE_MIN_SLEEP_TIME
            runningSince = System.currentTimeMillis()
            val result = activity.process(dataSig.res, activitySignal.params)
            println(actId + "- Processed " + dataSig.res + " --> " + result)
            insertNewResult(result, activitySignal, actId, dataSig.injectID)
          } else {
            // if nothing was found, it will sleep for a while
            Thread.sleep(sleepTime)
            sleepTime = math.min(sleepTime * 2, NODE_MAX_SLEEP_TIME)
          }

          //update space with current function
          signalSpace.write(templaceUpdateAct, NODE_INFO_LEASE_TIME)

          //checks if its need to change mode
          transformNode(actId)
      }
    }

    def transformNode(actId: String) = {
      val nowTime = System.currentTimeMillis()
      if (nowTime - checkTime > NODE_CHECK_TABLE_TIME) {
        val tableEntry = signalSpace.read(templateTable, ENTRY_READ_TIME)
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
      val entry = signalSpace.read(templateAct, ENTRY_READ_TIME)
      if (entry == null) {
        Log.error("Unknown Activity: " + activityId)
      } else entry.payload match {
        case activitySignal: ActivitySignal =>
          ActivityCache.getActivity(activitySignal.name) match {
            case Some(activity) =>
              templaceUpdateAct.payload = (nodeId, activityId, NODE_INFO_EXPIRY_TIME)
              templateData.marker = activityId
              worker = Some(activity, activityId, activitySignal)
            case None => Log.error("Activity not found in JarSpace: " + activitySignal.name)
          }
      }
    }

    def insertNewResult(result: Serializable, actSig: ActivitySignal, actId: String, injId: String) = {
      val to = actSig.outMarkers.head
      val tmplInsert = new ExoEntry(to, DataSignal(to, actId, result, injId))
      dataSpace.write(tmplInsert, DATA_LEASE_TIME)
    }

  }

}
