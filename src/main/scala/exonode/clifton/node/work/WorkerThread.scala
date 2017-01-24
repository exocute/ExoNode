package exonode.clifton.node.work

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}
import java.io.Serializable

import exonode.clifton.Protocol._
import exonode.clifton.node.entries.DataEntry
import exonode.clifton.node.{CliftonNode, Log, SpaceCache}

/**
  * this thread is continually running till be shutdown
  * it process the input at the same that allows the node to handle signals
  */
class WorkerThread(node: CliftonNode) extends Thread with Worker with BusyWorking {

  private val dataSpace = SpaceCache.getDataSpace

  private type QueueType = (ActivityWorker, Vector[DataEntry])
  private val queue: BlockingQueue[QueueType] = new LinkedBlockingQueue[QueueType]()
  private var isBusy = false

  override def threadIsBusy: Boolean = isBusy

  override def sendInput(activity: ActivityWorker, input: Vector[DataEntry]): Unit = {
    isBusy = true
    queue.add((activity, input))
  }

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
        println("Message: " + e.getMessage)
        Log.error("Message: " + e.getMessage)
    }
  }

  def process(activity: ActivityWorker, dataEntries: Vector[DataEntry]): Unit = {
    val input: Vector[Serializable] = dataEntries.map(_.data)
    val runningSince = System.currentTimeMillis()
    Log.info(s"Node ${node.nodeId}(${activity.id}) started processing")

    val result = {
      if (input.size == 1)
        activity.process(input.head)
      else
        activity.process(input)
    }
    Log.info(s"Node ${node.nodeId}(${activity.id}) finished processing in ${System.currentTimeMillis() - runningSince}ms")
    insertNewResult(result, activity.id, dataEntries, activity.acsTo)
    println(s"Node ${node.nodeId}(${activity.id}) Result " + result.toString.take(50) + "...")
  }

  def insertNewResult(result: Serializable, actId: String, dataEntries: Vector[DataEntry], actsTo: Vector[String]): Unit = {
    val injId = dataEntries.head.injectId
    for (actTo <- actsTo) {
      val dataEntry = DataEntry(actTo, actId, injId, result)
      dataSpace.write(dataEntry, DATA_LEASE_TIME)
    }

    //clear Backups
    for(dataEntry <- dataEntries){
      dataSpace.takeMany(dataEntry.createBackup(),MAX_BACKUPS_IN_SPACE)
      dataSpace.takeMany(dataEntry.createInfoBackup(),MAX_BACKUPS_IN_SPACE)
    }
  }

}