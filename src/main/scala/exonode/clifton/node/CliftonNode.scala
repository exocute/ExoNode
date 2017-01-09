package exonode.clifton.node

import java.io.Serializable
import java.util.UUID

import exonode.clifton.Protocol
import exonode.clifton.Protocol._
import exonode.clifton.signals.ActivitySignal
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

  //templates to search in space
  val templateAct = new ExoEntry("", null)
  val templateTable = new ExoEntry(TABLE_MARKER, null)
  var templateData: DataEntry = new DataEntry
  val templateUpdateAct = new ExoEntry(INFO_MARKER, null) // (idNode: String, idAct: String, valid: Long)

  override def run(): Unit = {

    //val bootTime = System.currentTimeMillis()

    //current worker definitions
    var worker: Option[(Activity, String, ActivitySignal)] = None

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
            val table = tableEntry.payload.asInstanceOf[TableType]
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
          val dataEntry = dataSpace.take(templateData, ENTRY_READ_TIME)
          if (dataEntry != null) {
            //if something was found
            idleTime = 0
            sleepTime = NODE_MIN_SLEEP_TIME
            runningSince = System.currentTimeMillis()
            Log.info(s"Node $nodeId($actId) started to process")
            val result = activity.process(dataEntry.data, activitySignal.params)
            insertNewResult(result, activitySignal, actId, dataEntry.injectId)
            Log.info(s"Node $nodeId($actId) finished to process in ${System.currentTimeMillis() - runningSince}")
          } else {
            // if nothing was found, it will sleep for a while
            Thread.sleep(sleepTime)
            sleepTime = math.min(sleepTime * 2, NODE_MAX_SLEEP_TIME)
          }

          //update space with current function
          signalSpace.write(templateUpdateAct, NODE_INFO_LEASE_TIME)

          //checks if its need to change mode
          transformNode(actId)
      }
    }

    def transformNode(actId: String) = {
      val nowTime = System.currentTimeMillis()
      if (nowTime - checkTime > NODE_CHECK_TABLE_TIME) {
        val tableEntry = signalSpace.read(templateTable, ENTRY_READ_TIME)
        if (tableEntry != null) {
          val table = tableEntry.payload.asInstanceOf[TableType]
          val totalNodes = table.values.sum
          val n = 1.0 / (table.size - 1)
          val q = table(actId).toDouble / totalNodes
          val uw = 1.0 / totalNodes
          //checks if its need to update function
          if (q > n && Random.nextDouble() < (q - n) / q) {
            //should i transform
            val newAct = getRandomActivity(table)
            val qnew = table(newAct).toDouble / totalNodes
            //            println(q, qnew, uw, n, q - uw >= n, qnew + uw <= n)
            if (newAct != actId && (q - uw >= n || qnew + uw <= n)) {
              setActivity(newAct)
            }
          }
        }
        checkTime = System.currentTimeMillis()
      }
    }

    def getRandomActivity(table: TableType): String = {
      val filteredList: List[TableEntryType] = table.toList.filterNot(_._1 == ANALISER_ACT_ID)
      val total = filteredList.unzip._2.sum
      val n = 1.0 / filteredList.size
      val list: List[TableEntryType] = filteredList.filter(_._2.toDouble / total < n)

      if (list.isEmpty)
        filteredList(Random.nextInt(filteredList.size))._1
      else
        list(Random.nextInt(list.size))._1
    }

    def setActivity(activityId: String): Unit = {
      templateAct.marker = activityId
      val entry = signalSpace.read(templateAct, ENTRY_READ_TIME)
      if (entry == null) {
        Log.error("Activity not found in JarSpace: " + activityId)
        Thread.sleep(Protocol.ACT_NOT_FOUND_SLEEP_TIME)
      } else entry.payload match {
        case activitySignal: ActivitySignal =>
          ActivityCache.getActivity(activitySignal.name) match {
            case Some(activity) =>
              templateUpdateAct.payload = (nodeId, activityId, NODE_INFO_EXPIRY_TIME)
              templateData = templateData.setTo(activityId)
              Log.info(s"Node $nodeId changed from " +
                s"${if (worker.isDefined) worker.get._2 else Protocol.UNDEFINED_ACT_ID} to $activityId")
              worker = Some(activity, activityId, activitySignal)
            case None =>
              Log.error("Class could not be loaded: " + activitySignal.name)
              Thread.sleep(Protocol.ACT_NOT_FOUND_SLEEP_TIME)
          }
      }
    }

    def insertNewResult(result: Serializable, actSig: ActivitySignal, actId: String, injId: String) = {
      for (to <- actSig.outMarkers) {
        val tmplInsert = new DataEntry(to, actId, injId, result)
        dataSpace.write(tmplInsert, DATA_LEASE_TIME)
      }
    }
  }

}
