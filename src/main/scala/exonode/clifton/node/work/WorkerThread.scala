package exonode.clifton.node.work

import java.io.Serializable
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import exonode.clifton.config.BackupConfig
import exonode.clifton.config.Protocol._
import exonode.clifton.node.Log.{INFO, ND, WARN}
import exonode.clifton.node.entries.{DataEntry, FlatMapEntry}
import exonode.clifton.node.{CliftonNode, Log, Node, SpaceCache}
import exonode.clifton.signals.{ActivityFilterType, ActivityFlatMapType, ActivityMapType, LoggingSignal}

/**
  * Created by #GrowinScala
  *
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
      case e: Throwable =>
        val msg = "Message: " + e.toString + ", " + e.getStackTrace.mkString(", ")
        println(nodeId + ";" + msg)
        Log.receiveLog(LoggingSignal(LOGCODE_ERROR_PROCESSING, WARN, nodeId, ND, ND, ND, ND, msg, 0))
    }
  }

  def getGraphID(s: String): String = {
    s.split(':').head
  }

  def process(activity: ActivityWorker, dataEntries: Vector[DataEntry]): Unit = {
    val runningSince = System.currentTimeMillis()
    Log.receiveLog(LoggingSignal(LOGCODE_PROCESSING_INPUT, INFO, nodeId, getGraphID(activity.id),
      ND, activity.id, dataEntries.head.injectId, "Node started processing", 0))

    val result: Either[Option[Seq[Serializable]], Option[Serializable]] = {
      if (dataEntries.exists(dataEntry => dataEntry.data.isEmpty)) {
        if (activity.actType == ActivityFlatMapType) Left(None) else Right(None)
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
          case ActivityMapType => Right(Some(processResult))
          case ActivityFilterType => processResult match {
            case filterResult: java.lang.Boolean => Right(if (filterResult) Some(input) else None)
            case _ => Right(None)
          }
          case ActivityFlatMapType => processResult match {
            case seq =>
              Left(Some(seq.asInstanceOf[Seq[Serializable]]))
          }
        }
      }
    }

    val timeProcessing = System.currentTimeMillis() - runningSince
    Log.receiveLog(LoggingSignal(LOGCODE_FINISHED_PROCESSING, INFO, nodeId, getGraphID(activity.id), activity.id,
      activity.acsTo.head.split(':').last, dataEntries.head.injectId,
      s"Node finished processing in ${timeProcessing}ms", timeProcessing))
    if (activity.actType == ActivityFlatMapType)
      insertResultFlatten(result.left.get, dataEntries, activity)
    else
      insertResultStandard(result.right.get, dataEntries, activity)
    CliftonNode.debug(nodeId, s"$nodeId(${activity.id});Result: " + result.fold(identity, identity).toString.take(50) + "...")
  }

  private def insertResultFlatten(result: Option[Seq[Serializable]],
                                  dataEntries: Vector[DataEntry],
                                  activityWorker: ActivityWorker): Unit = {

    val actId = activityWorker.id
    val actsTo = activityWorker.acsTo
    val injId = dataEntries.head.injectId

    def sendResult(actTo: String, orderId: String, result: Option[Serializable]): Unit = {
      dataSpace.write(DataEntry(actTo, actId, injId, orderId, result), DATA_LEASE_TIME)
    }

    result match {
      case None =>
        // Send None to each activity of the fork
        val orderId = s"${dataEntries.head.orderId}:0"
        for (actTo <- actsTo)
          sendResult(actTo, orderId, None)
        // The collector only needs to know that there is one element (that is filtered)
        dataSpace.write(FlatMapEntry.fromInjectId(injId, orderId, 1), DATA_LEASE_TIME)
      case Some(dataSeq) =>
        // Send the values seq to each activity of the fork
        val orderId = dataEntries.head.orderId
        for {
          (value, index) <- dataSeq.zipWithIndex
          actTo <- actsTo
        } sendResult(actTo, s"$orderId:$index", Some(value))
        // The collector needs to know how many elements there are
        dataSpace.write(FlatMapEntry.fromInjectId(injId, orderId, dataSeq.size), DATA_LEASE_TIME)
    }

    clearBackups(dataEntries)
  }

  private def insertResultStandard(result: Option[Serializable],
                                   dataEntries: Vector[DataEntry],
                                   activityWorker: ActivityWorker): Unit = {
    val actId = activityWorker.id
    val actsTo = activityWorker.acsTo
    val injId = dataEntries.head.injectId
    val orderId = dataEntries.head.orderId // same order id, because we are not in a flatMap

    for (actTo <- actsTo) {
      val dataEntry = DataEntry(actTo, actId, injId, orderId, result)
      dataSpace.write(dataEntry, DATA_LEASE_TIME)
    }

    clearBackups(dataEntries)
  }

  private def clearBackups(dataEntries: Vector[DataEntry]) {
    for (dataEntry <- dataEntries) {
      dataSpace.takeMany(dataEntry.createBackup(), backupConfig.MAX_BACKUPS_IN_SPACE)
      dataSpace.takeMany(dataEntry.createInfoBackup(), backupConfig.MAX_BACKUPS_IN_SPACE)
    }
  }

}