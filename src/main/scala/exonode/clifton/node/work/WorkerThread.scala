package exonode.clifton.node.work

import java.io.Serializable
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import exonode.clifton.config.BackupConfig
import exonode.clifton.config.Protocol._
import exonode.clifton.node.Log.{INFO, ND, WARN}
import exonode.clifton.node.entries.DataEntry
import exonode.clifton.node.{CliftonNode, Log, Node, SpaceCache}
import exonode.clifton.signals.{ActivityFilterType, ActivityFlatMapType, ActivityMapType, LoggingSignal}

/**
  * This thread is continually running till be shutdown
  * it processes input at the same time that the node continues to handle signals
  */
class WorkerThread(node: Node)(implicit backupConfig: BackupConfig) extends Thread with BusyWorking with Worker {

  private val dataSpace = SpaceCache.getDataSpace

  private type QueueType = (ActivityWorker, Vector[DataEntry])
  private val queue: BlockingQueue[QueueType] = new LinkedBlockingQueue[QueueType]()
  private var isBusy = false

  override def threadIsBusy: Boolean = isBusy

  override def sendInput(activity: ActivityWorker, input: Vector[DataEntry]): Unit = {
    isBusy = true
    queue.add((activity, input))
  }

  val nodeId: String = node.nodeId

  override def run(): Unit = {
    try {
      while (true) {
        val (activity, input) = queue.take()
        process(activity, input)
        isBusy = false
        //TODO: find a better way to notify the node that we finished processing
        node.finishedProcessing()
      }
    } catch {
      case e: InterruptedException =>
        println("InterruptedException: " + e.getMessage)
      case e: RuntimeException =>
        val msg = "Message: " + e.toString + ", " + e.getStackTrace.mkString(", ")
        println(nodeId + ";" + msg)
        Log.receiveLog(LoggingSignal(LOGCODE_ERROR_PROCESSING, WARN, nodeId, ND, ND, ND, ND, msg, 0))

      case e: Throwable =>
        val msg = "Message: " + e.toString
        println(nodeId + ";" + msg)
        Log.receiveLog(LoggingSignal(LOGCODE_ERROR_PROCESSING, WARN, nodeId, ND, ND, ND, ND, msg, 0))
    }
  }

  def getGraphID(s: String): String = {
    s.split(':').head
  }

  def process(activity: ActivityWorker, dataEntries: Vector[DataEntry]): Unit = {
    val runningSince = System.currentTimeMillis()
    Log.receiveLog(LoggingSignal(LOGCODE_PROCESSING_INPUT, INFO, nodeId, getGraphID(activity.id), ND, activity.id, dataEntries.head.injectId, "Node started processing", 0))

    val result: Option[Serializable] = {
      if (dataEntries.exists(dataEntry => dataEntry.data.isEmpty)) {
        None
      } else {
        val input: Serializable = {
          val entriesData = dataEntries.map(_.data.get)
          if (entriesData.size == 1)
            entriesData.head
          else
            entriesData
        }
        val processResult = activity.process(input)
        activity.actType match {
          case ActivityMapType => Some(processResult)
          case ActivityFilterType => processResult match {
            case filterResult: java.lang.Boolean => if (filterResult) Some(input) else None
            case _ => None
          }
          case ActivityFlatMapType =>
            //FIXME its the same as map for now
            Some(processResult)
        }
      }
    }

    //    println(s"Result of $nodeId(${activity.id}): $result")

    //TODO: add logs to filter and flatmap intermediate results ...

    val timeProcessing = System.currentTimeMillis() - runningSince
    Log.receiveLog(LoggingSignal(LOGCODE_FINISHED_PROCESSING, INFO, nodeId, getGraphID(activity.id), activity.id,
      activity.acsTo.head.split(':').last, dataEntries.head.injectId,
      s"Node finished processing in ${timeProcessing}ms", timeProcessing))
    insertNewResult(result, dataEntries, activity)
    CliftonNode.debug(nodeId, s"$nodeId(${activity.id});Result: " + result.toString.take(50) + "...")
  }

  def insertNewResult(result: Option[Serializable], dataEntries: Vector[DataEntry], activityWorker: ActivityWorker): Unit = {
    val actId = activityWorker.id
    val actsTo = activityWorker.acsTo
    val injId = dataEntries.head.injectId
    val newOrderId = dataEntries.head.orderId //FIXME add flatMap new id if necessary
    if (actsTo.size == 1) {
      val actTo = actsTo.head
      val dataEntry = DataEntry(actTo, actId, injId, newOrderId, result)
      dataSpace.write(dataEntry, DATA_LEASE_TIME)
    } else {
      val resultVector = result match {
        case Some(values) => values.asInstanceOf[IndexedSeq[Serializable]].map(Some(_))
        case None => actsTo.map(_ => None)
      }

      for (index <- actsTo.indices) {
        val v = resultVector(index)
        val dataEntry = DataEntry(actsTo(index), actId, injId, newOrderId, v)
        dataSpace.write(dataEntry, DATA_LEASE_TIME)
      }
    }

    //clear Backups
    for (dataEntry <- dataEntries) {
      dataSpace.takeMany(dataEntry.createBackup(), backupConfig.MAX_BACKUPS_IN_SPACE)
      dataSpace.takeMany(dataEntry.createInfoBackup(), backupConfig.MAX_BACKUPS_IN_SPACE)
    }
  }

}