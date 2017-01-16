package exonode.clifton.node

import java.io.Serializable
import java.util.UUID

import exonode.clifton.Protocol._
import exonode.clifton.signals.{ActivitySignal, KillGracefullSignal, KillSignal, NodeSignal}
import exonode.exocuteCommon.activity.Activity

import scala.util.Random
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}

/**
  * Created by #ScalaTeam on 05-01-2017.
  *
  * Generic node. Nodes are responsible for processing activities or analyse some INFOs from the space
  * CliftonNode has two main modes: Analyser or Worker
  * Analyser is responsible for updating the space with a table about the state of every node
  * Worker is responsible for processing input for some activity
  */
class CliftonNode extends Thread {

  private val nodeId: String = UUID.randomUUID().toString

  private val signalSpace = SpaceCache.getSignalSpace
  private val dataSpace = SpaceCache.getDataSpace

  //templates to search in space
  val templateAct = new ExoEntry("", null)
  val templateTable = new ExoEntry(TABLE_MARKER, null)
  var templateData: DataEntry = new DataEntry
  val templateUpdateAct = new ExoEntry(INFO_MARKER, null)
  // (idNode: String, idAct: String, valid: Long)
  val templateMySignals = new ExoEntry(nodeId, null)
  val templateNodeSignals = new ExoEntry(NODE_SIGNAL_MARKER, null)

  def updateNodeInfo(): Unit = {
    //update space with current function
    signalSpace.write(templateUpdateAct, NODE_INFO_LEASE_TIME)
  }

  def hasAnalyser(tab: TableType): Boolean = {
    tab.get(ANALYSER_ACT_ID) match {
      case Some(analyserCount) => analyserCount != 0
      case None => true
    }
  }


  override def run(): Unit = {

    //current worker definitions
    var worker: Work = NoWork
    var processing: Option[BusyWorking] = None
    var waitQueue: BlockingQueue[Unit] = new LinkedBlockingQueue[Unit]()

    //times initializer
    var sleepTime = NODE_MIN_SLEEP_TIME
    var checkTime = System.currentTimeMillis()
    var killWhenIdle = false

    println(s"Node $nodeId is ready to start")
    Log.info(s"Node $nodeId is ready to start")
    while (true) {
      handleSignals()

      if (killWhenIdle) {
        killOwnSignal()
      }

      worker match {
        // worker is not defined yet
        case NoWork =>
          val tableEntry = signalSpace.read(templateTable, ENTRY_READ_TIME)
          if (tableEntry != null) {
            if (!tryToBeAnaliser(tableEntry)) {
              sleepTime = NODE_MIN_SLEEP_TIME
              val table = tableEntry.payload.asInstanceOf[TableType]
              val act = getRandomActivity(table)
              setActivity(act)
              updateNodeInfo()
            }
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

    /**
      * Receives and tableEntry and checks if the position where analyser node is defined has zero
      * If true, tries to take the table from the space and if it gets it, starts the analyser mode
      * Once analyser mode is started the table is updated in space with the info that already exists
      * one clifton node in analyser mode
      * @param tableEntry
      * @return true if it was transformed in analyser node, false otherwise
      */
    def tryToBeAnaliser(tableEntry: ExoEntry): Boolean = {
      if (!hasAnalyser(tableEntry.payload.asInstanceOf[TableType])) {
        val tabEntry = signalSpace.take(templateTable, ENTRY_READ_TIME)
        if (tabEntry != null) {
          val tab = tabEntry.payload.asInstanceOf[TableType]
          if (!hasAnalyser(tab)) {
            val analiserThread = new Thread with BusyWorking {
              override def threadIsBusy = true

              override def run(): Unit = {
                new AnalyserNode(nodeId, tab).startAnalysing()
              }
            }
            analiserThread.start()
            processing = Some(analiserThread)
            return true
          } else
            signalSpace.write(tableEntry, TABLE_LEASE_TIME)
        }
      }
      false
    }

    /**
      * Reads signals from the space that be general to every node or specific to some node with
      * a defined ID
      */
    def handleSignals(): Unit = {
      val mySignalEntry = signalSpace.take(templateMySignals, ENTRY_READ_TIME)
      if (mySignalEntry != null) {
        val mySignal = mySignalEntry.payload.asInstanceOf[NodeSignal]
        processSignal(mySignal)
      }
      val nodeSignalEntry = signalSpace.take(templateNodeSignals, ENTRY_READ_TIME)
      if (nodeSignalEntry != null) {
        val nodeSignal = nodeSignalEntry.payload.asInstanceOf[NodeSignal]
        processSignal(nodeSignal)
      }
    }

    /**
      * kills the current node
      */
    def killOwnSignal(): Unit = {
      Log.info(s"Node $nodeId is going to shutdown")
      processing match {
          // no more work(processing activities) is done
        case Some(thread: Thread) => while (true) thread.interrupt()
        case _ => ()
      }
      //aborts the current node
      while (true) Thread.currentThread().interrupt()
    }

    /**
      * if its a KillSignal the node immediately aborts and dies, if its a KillGraceFullSignal
      * killWhenIdle is changed to true and before receiving something new to process the node dies
      * @param nodeSignal
      */
    def processSignal(nodeSignal: NodeSignal) = {
      nodeSignal match {
        case KillSignal => killOwnSignal()
        case KillGracefullSignal =>
          killWhenIdle = true
      }
    }

    /**
      * try to read all the results of an injectID that should do a Join
      * @param actsFrom
      * @return true if both activities of the join are already present in the space
      */
    def tryToReadAll(actsFrom: Vector[String]): Boolean = {

      val from = actsFrom(0)
      val dataEntry = dataSpace.read(templateData.setFrom(from).setInjectId(null), ENTRY_READ_TIME)
      if (dataEntry == null) {
        false
      } else {
        templateData = templateData.setInjectId(dataEntry.injectId)
        for(act <- 1 until actsFrom.size) {
          val dataEntry = dataSpace.read(templateData.setFrom(actsFrom(act)), ENTRY_READ_TIME)
          if (dataEntry == null)
            false
        }
        true
      }
    }

    /**
      * try to read all the results of an injectID that should do a Join
      * @param actsFrom
      * @return if both activities of the join was successfully taken from the space it returns
      *         a vector with them
      */
    def tryToTakeAll(actsFrom: Vector[String]): Vector[DataEntry] = {

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
            tryToTakeAllAux(index + 1, acc :+ dataEntry)
          }
        }
      }

      tryToTakeAllAux(0, Vector())
    }

    /**
      * if a worker is already defined we send the input to be processed
      * else, a new thread is created and then the input is send
      * @param dataEntry
      * @param activity
      * @param actsTo
      */
    def process(dataEntry: Vector[DataEntry], activity: ActivityWorker, actsTo: Vector[String]) = {
      val thread = processing match {
        case Some(workerThread: Worker) =>
          workerThread.sendInput(dataEntry)
          workerThread
        case _ =>
          val workerThread = new WorkerThread(activity, actsTo, waitQueue)
          workerThread.start()
          processing = Some(workerThread)
          workerThread.sendInput(dataEntry)
          workerThread
      }

      while (thread.threadIsBusy) {
        handleSignals()
        try {
          waitQueue.poll(1000, TimeUnit.MILLISECONDS)
        } catch {
          case _: InterruptedException =>
            // thread finished processing
            // So, back to normal mode
        }
      }
    }

    /**
      * checks in period of time if change its needed
      * @param actId
      */
    def checkNeedToChange(actId: String) = {
      val nowTime = System.currentTimeMillis()
      if (nowTime - checkTime > NODE_CHECK_TABLE_TIME) {
        val tableEntry = signalSpace.read(templateTable, ENTRY_READ_TIME)
        if (tableEntry != null) {
          //first checks is a table already contains an analyser
          if (!tryToBeAnaliser(tableEntry)) {
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
        }
        checkTime = nowTime
        updateNodeInfo()
      }
    }

    /**
      * @param table
      * @return returns a random act_id taken from the table
      */
    def getRandomActivity(table: TableType): String = {
      val filteredList: List[TableEntryType] = table.toList.filterNot(_._1 == ANALYSER_ACT_ID)
      val total = filteredList.unzip._2.sum
      val n = 1.0 / filteredList.size
      val list: List[TableEntryType] = filteredList.filter(_._2.toDouble / total < n)

      if (list.isEmpty)
        filteredList(Random.nextInt(filteredList.size))._1
      else
        list(Random.nextInt(list.size))._1
    }

    /**
      * sets the node to process input to a specific activity
      * updates the templates and worker
      * @param activityId
      */
    def setActivity(activityId: String): Unit = {
      templateAct.marker = activityId
      val entry = signalSpace.read(templateAct, ENTRY_READ_TIME)
      if (entry == null) {
        Log.error("Activity not found in JarSpace: " + activityId)
        Thread.sleep(ACT_NOT_FOUND_SLEEP_TIME)
      } else entry.payload match {
        case activitySignal: ActivitySignal =>
          ActivityCache.getActivity(activitySignal.name) match {
            case Some(activity) =>
              templateUpdateAct.payload = (nodeId, activityId, NODE_INFO_EXPIRY_TIME)
              templateData = new DataEntry().setTo(activityId)
              val activityWorker = new ActivityWorker(activityId, activity, activitySignal.params)
              val fromId = if (worker.hasWork) worker.activity.id else UNDEFINED_ACT_ID
              activitySignal.inMarkers match {
                case Vector(_) =>
                  worker = PipeWork(activityWorker, activitySignal.outMarkers)
                case actFrom: Vector[String] =>
                  worker = JoinWork(activityWorker, actFrom, activitySignal.outMarkers)
              }
              Log.info(s"Node $nodeId changed from $fromId to $activityId")
            case None =>
              Log.error("Class could not be loaded: " + activitySignal.name)
              Thread.sleep(ACT_NOT_FOUND_SLEEP_TIME)
          }
      }
    }
  }

  trait BusyWorking {
    def threadIsBusy: Boolean
  }

  trait Worker {
    def sendInput(input: Vector[DataEntry]): Unit
  }

  /**
    * this thread is continually running till be shutdown
    * it process the input at the same that allows the node to handle signals
    * @param activity
    * @param actsTo
    * @param callback
    */
  class WorkerThread(activity: ActivityWorker, actsTo: Vector[String], callback: BlockingQueue[Unit]) extends Thread with Worker with BusyWorking {

    val queue: BlockingQueue[Vector[DataEntry]] = new LinkedBlockingQueue[Vector[DataEntry]]()
    var isProcessing = false

    override def threadIsBusy: Boolean = isProcessing

    override def sendInput(input: Vector[DataEntry]): Unit = queue.add(input)

    override def run(): Unit = {
      try {
        while (true) {
          val input = queue.take()
          isProcessing = true
          process(input)
          isProcessing = false
          callback.add(())
        }
      } catch {
        case e: InterruptedException =>
          println("InterruptedException: " + e.getMessage)
        case e: Throwable =>
          println("Message: " + e.getMessage)
      }
    }

    def process(dataEntry: Vector[DataEntry]): Unit = {
      val input: Vector[Serializable] = dataEntry.map(_.data)
      val runningSince = System.currentTimeMillis()
      Log.info(s"Node $nodeId(${activity.id}) started processing")

      val result = {
        if (input.size == 1)
          activity.process(input.head)
        else
          activity.process(input)
      }
      Log.info(s"Node $nodeId(${activity.id}) finished processing in ${System.currentTimeMillis() - runningSince}ms")
      insertNewResult(result, activity.id, dataEntry.head.injectId, actsTo)
      println(s"Node $nodeId(${activity.id}) Result " + result)
    }

    def insertNewResult(result: Serializable, actId: String, injId: String, actsTo: Vector[String]): Unit = {
      for (actTo <- actsTo) {
        val dataEntry = new DataEntry(actTo, actId, injId, result)
        dataSpace.write(dataEntry, DATA_LEASE_TIME)
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

}
