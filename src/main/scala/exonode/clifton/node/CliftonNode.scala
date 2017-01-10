package exonode.clifton.node

import java.io.Serializable
import java.util.UUID

import exonode.clifton.Protocol
import exonode.clifton.Protocol._
import exonode.clifton.signals.ActivitySignal
import exonode.exocuteCommon.activity.Activity

import scala.collection.immutable.HashMap
import scala.concurrent.Future
import scala.util.{Random, Try}

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

  def updateNodeInfo() = {
    //update space with current function
    signalSpace.write(templateUpdateAct, NODE_INFO_LEASE_TIME)
  }

  override def run(): Unit = {

    //    val bootTime = System.currentTimeMillis()

    //current worker definitions
    var worker: Work = NoWork
    var processing: Option[Future[Try[Unit]]] = None

    //times initializer
    //    var idleTime = System.currentTimeMillis()
    var runningSince = 0L
    var sleepTime = NODE_MIN_SLEEP_TIME
    var checkTime = System.currentTimeMillis()

    println(s"Node $nodeId is ready to start")
    Log.info(s"Node $nodeId is ready to start")
    while (true) {
      worker match {
        // worker is not defined yet
        case NoWork =>
          val tableEntry = signalSpace.read(templateTable, ENTRY_READ_TIME)
          if (tableEntry != null) {
            sleepTime = NODE_MIN_SLEEP_TIME
            val table = tableEntry.payload.asInstanceOf[TableType]
            val act = getRandomActivity(table)
            setActivity(act)
            updateNodeInfo()
          } else {
            // if nothing was found, it will sleep for a while
            Thread.sleep(sleepTime)
            sleepTime = math.min(sleepTime * 2, NODE_MAX_SLEEP_TIME)
          }

        //worker is a join
        case JoinWork(activity, actsFrom, actsTo) =>

          if (tryToReadAll(Random.shuffle(actsFrom))) {
            tryToTakeAll(actsFrom) match {
              case Vector() =>
              // Other node was faster to take the data
              // Just restart the process
              case values: Vector[DataEntry] =>
                if (values.size == actsFrom.size) {
                  // we have all values, so we can continue
                  //                  idleTime = 0
                  sleepTime = NODE_MIN_SLEEP_TIME
                  process(values, activity, actsTo)
                } else {
                  // some values were lost ?
                  Log.error(s"Data was missing from node $nodeId(${activity.id})" +
                    s" with injectId=${values.head.injectId}, ${values.size} values found, ${actsFrom.size} values expected")
                }
            }
          } else {
            // if nothing was found, it will sleep for a while
            Thread.sleep(sleepTime)
            sleepTime = math.min(sleepTime * 2, NODE_MAX_SLEEP_TIME)
          }

          //checks if its need to change mode
          checkNeedToChange(activity.id)
        case PipeWork(activity, actsTo) =>
          //get something to process
          val dataEntry = dataSpace.take(templateData, ENTRY_READ_TIME)
          if (dataEntry != null) {
            //if something was found
            //            idleTime = 0
            sleepTime = NODE_MIN_SLEEP_TIME
            process(Vector(dataEntry), activity, actsTo)
          } else {
            // if nothing was found, it will sleep for a while
            Thread.sleep(sleepTime)
            sleepTime = math.min(sleepTime * 2, NODE_MAX_SLEEP_TIME)
          }

          //checks if its need to change mode
          checkNeedToChange(activity.id)
      }
    }

    def tryToReadAll(actsFrom: Vector[String]): Boolean = {
      def tryToReadAllAux(index: Int): Boolean = {
        if (index >= actsFrom.size)
          true
        else {
          val from = actsFrom(index)
          val dataEntry = dataSpace.read(templateData.setFrom(from), ENTRY_READ_TIME)
          if (dataEntry == null) {
            false
          } else {
            tryToReadAllAux(index + 1)
          }
        }
      }

      val from = actsFrom(0)
      val dataEntry = dataSpace.read(templateData.setFrom(from).setInjectId(null), ENTRY_READ_TIME)
      if (dataEntry == null) {
        false
      } else {
        templateData = templateData.setInjectId(dataEntry.injectId)
        tryToReadAllAux(1)
      }
    }

    def tryToTakeAll(actsFrom: Vector[String] /*, injectId: String*/): Vector[DataEntry] = {
      def tryToTakeAllAux(index: Int, acc: Vector[DataEntry]): Vector[DataEntry] = {
        if (index >= actsFrom.size) {
          acc
        }
        else {
          val from = actsFrom(index)
          val dataEntry = dataSpace.take(templateData.setFrom(from), ENTRY_READ_TIME)
          if (dataEntry == null) {
            acc // if acc.size > 0 then some error happened
          } else {
            println(s"Node $nodeId take ${dataEntry.injectId} from ${dataEntry.fromAct}")
            tryToTakeAllAux(index + 1, acc :+ dataEntry)
          }
        }
      }

      //      templateData = templateData.setInjectId(injectId)
      println(s"Node $nodeId started to take")
      tryToTakeAllAux(0, Vector())
    }

    //    def process(dataEntry: DataEntry, activity: ActivityWorker, actsTo: Vector[String]) = {
    //      runningSince = System.currentTimeMillis()
    //      Log.info(s"Node $nodeId(${activity.id}) started to process")
    //      try {
    //        val result = activity.process(dataEntry.data)
    //        Log.info(s"Node $nodeId(${activity.id}) finished to process in ${System.currentTimeMillis() - runningSince}")
    //        insertNewResult(result, activity.id, dataEntry.injectId, actsTo)
    //      } catch {
    //        case e: Throwable =>
    //          Log.error(s"Node $nodeId(${activity.id}) thrown an error: ${e.getMessage}")
    //      }
    //    }

    def process(dataEntry: Vector[DataEntry], activity: ActivityWorker, actsTo: Vector[String]) = {
      val input: Vector[Serializable] = dataEntry.map(_.data)
      runningSince = System.currentTimeMillis()
      Log.info(s"Node $nodeId(${activity.id}) started to process")
      try {
        val result = {
          if (input.size == 1)
            activity.process(input.head)
          else
            activity.process(input)
        }
        Log.info(s"Node $nodeId(${activity.id}) finished to process in ${System.currentTimeMillis() - runningSince}ms")
        insertNewResult(result, activity.id, dataEntry.head.injectId, actsTo)
        println(s"Node $nodeId(${activity.id}) Result " + result)
      } catch {
        case e: Throwable =>
          Log.error(s"Node $nodeId(${activity.id}) thrown an error: ${e.getMessage}")
      }
    }

    def checkNeedToChange(actId: String) = {
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
        checkTime = nowTime
        updateNodeInfo()
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
              templateData = new DataEntry().setTo(activityId)
              val activityWorker = new ActivityWorker(activityId, activity, activitySignal.params)
              val fromId = if (worker.hasWork) worker.activity.id else Protocol.UNDEFINED_ACT_ID
              activitySignal.inMarkers match {
                case Vector(singleAct) =>
                  worker = PipeWork(activityWorker, activitySignal.outMarkers)
                case actFrom =>
                  worker = JoinWork(activityWorker, actFrom, activitySignal.outMarkers)
              }
              Log.info(s"Node $nodeId changed from $fromId to $activityId")
            case None =>
              Log.error("Class could not be loaded: " + activitySignal.name)
              Thread.sleep(Protocol.ACT_NOT_FOUND_SLEEP_TIME)
          }
      }
    }

    def insertNewResult(result: Serializable, actId: String, injId: String, actsTo: Vector[String]) = {
      for (actTo <- actsTo) {
        val dataEntry = new DataEntry(actTo, actId, injId, result)
        dataSpace.write(dataEntry, DATA_LEASE_TIME)
      }
    }
  }

}

class ActivityWorker(val id: String, activity: Activity, params: Vector[String]) {
  @inline def process(input: Serializable): Serializable = activity.process(input, params)
}

sealed trait Work {
  def hasWork: Boolean

  def activity: ActivityWorker
}

case object NoWork extends Work {
  override def hasWork = false

  override def activity = throw new NoSuchElementException("NoWork.activity")
}

case class PipeWork(activity: ActivityWorker, actsTo: Vector[String]) extends Work {
  override def hasWork = true
}

case class JoinWork(activity: ActivityWorker, actsFrom: Vector[String], actsTo: Vector[String]) extends Work {
  override def hasWork = true
}