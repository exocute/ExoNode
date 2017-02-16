package exonode.clifton.node.work

import java.io.Serializable
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import exonode.clifton.config.BackupConfig
import exonode.clifton.config.Protocol._
import exonode.clifton.node.entries.DataEntry
import exonode.clifton.node.{Log, Node, SpaceCache}

/**
  * This thread is continually running till be shutdown
  * it process the input at the same that allows the node to handle signals
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
        val msg = "Message: " + e.getCause + ", " + e.getStackTrace.mkString(", ")
        println(nodeId + ";" + msg)
        Log.warn(nodeId, msg)
      case e: Throwable =>
        val msg = "Message: " + e.getMessage
        println(nodeId + ";" + msg)
        Log.warn(nodeId, msg)
    }
  }

  def process(activity: ActivityWorker, dataEntries: Vector[DataEntry]): Unit = {
    val input: Vector[Serializable] = dataEntries.map(_.data)
    val runningSince = System.currentTimeMillis()
    Log.info(s"$nodeId(${activity.id})", "Node started processing")

    val result = {
      if (input.size == 1)
        activity.process(input.head)
      else
        activity.process(input)
    }
    Log.info(s"$nodeId(${activity.id})", s"Node finished processing in ${System.currentTimeMillis() - runningSince}ms")
    insertNewResult(result, activity.id, dataEntries, activity.acsTo)
    println(s"$nodeId(${activity.id});Result " + result.toString.take(50) + "...")
  }

  def insertNewResult(result: Serializable, actId: String, dataEntries: Vector[DataEntry], actsTo: Vector[String]): Unit = {
    val injId = dataEntries.head.injectId
    if (actsTo.size == 1) {
      for (actTo <- actsTo) {
        val dataEntry = DataEntry(actTo, actId, injId, result)
        dataSpace.write(dataEntry, DATA_LEASE_TIME)
      }
    } else {
      val resultVector = result.asInstanceOf[Vector[Serializable]]
      for (index <- actsTo.indices) {
        val dataEntry = DataEntry(actsTo(index), actId, injId, resultVector(index))
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