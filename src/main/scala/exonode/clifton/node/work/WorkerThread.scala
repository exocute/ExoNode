package exonode.clifton.node.work

import java.io.Serializable
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import exonode.clifton.config.ProtocolConfig
import exonode.clifton.node.entries.{DataEntry, FlatMapEntry}
import exonode.clifton.node.{CliftonNode, Node, SpaceCache}
import exonode.clifton.signals.Log.{Log, LogErrorProcessing, LogFinishedProcessing, LogProcessingInput}
import exonode.clifton.signals.{ActivityFilterType, ActivityFlatMapType, ActivityMapType}

import scala.annotation.tailrec

/**
  * Created by #GrowinScala
  *
  * This thread is continually running till be shutdown
  * it processes input at the same time that the node continues to handle signals
  */
class WorkerThread(node: Node, val config: ProtocolConfig) extends Thread with BusyWorking with Worker {

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
      processNextInput()
    } catch {
      case e: InterruptedException =>
        println("InterruptedException: " + e.getMessage)
      case e: Throwable =>
        val msg = "Message: " + e.toString + ", " + e.getStackTrace.mkString(", ")
        println(nodeId + ";" + msg)
        Log.writeLog(LogErrorProcessing(nodeId, msg))
    }
  }

  @tailrec
  private def processNextInput(): Unit = {
    val (activity, input) = queue.take()
    process(activity, input)
    isBusy = false
    //TODO: find a better way to notify the node that we finished processing
    node.finishedProcessing()
    processNextInput()
  }

  private def process(activity: ActivityWorker, dataEntries: Vector[DataEntry]): Unit = {
    val runningSince = System.currentTimeMillis()
    Log.writeLog(LogProcessingInput(nodeId, dataEntries.head.fromAct, activity.id, dataEntries.head.injectId))

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
    Log.writeLog(LogFinishedProcessing(nodeId, activity.id,
      activity.acsTo.head.split(':').last, dataEntries.head.injectId, timeProcessing))
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
      dataSpace.write(DataEntry(actTo, actId, injId, orderId, result), config.DataLeaseTime)
    }

    result match {
      case None =>
        // Send None to each activity of the fork
        val orderId = s"${dataEntries.head.orderId}:0"
        for (actTo <- actsTo)
          sendResult(actTo, orderId, None)
        // The collector only needs to know that there is one element (that is filtered)
        dataSpace.write(FlatMapEntry.fromInjectId(injId, orderId, 1), config.DataLeaseTime)
      case Some(dataSeq) =>
        // Send the values seq to each activity of the fork
        val orderId = dataEntries.head.orderId
        for {
          (value, index) <- dataSeq.zipWithIndex
          actTo <- actsTo
        } sendResult(actTo, s"$orderId:$index", Some(value))
        // The collector needs to know how many elements there are
        dataSpace.write(FlatMapEntry.fromInjectId(injId, orderId, dataSeq.size), config.DataLeaseTime)
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
      dataSpace.write(dataEntry, config.DataLeaseTime)
    }

    clearBackups(dataEntries)
  }

  private def clearBackups(dataEntries: Vector[DataEntry]) {
    for (dataEntry <- dataEntries) {
      dataSpace.takeMany(dataEntry.createBackup(), config.MaxBackupsInSpace)
      dataSpace.takeMany(dataEntry.createInfoBackup(), config.MaxBackupsInSpace)
    }
  }

}