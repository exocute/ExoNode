package exonode.clifton.node

import java.util.UUID
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}

import exonode.clifton.Protocol._
import exonode.clifton.node.work.{ConsecutiveWork, _}
import exonode.clifton.signals.{ActivitySignal, KillGracefullSignal, KillSignal, NodeSignal}

import scala.util.Random

/**
  * Created by #ScalaTeam on 05-01-2017.
  *
  * Generic node. Nodes are responsible for processing activities or analysing INFOs from the space.
  * A CliftonNode has two main modes: Analyser or Worker
  * Analyser is responsible for updating the space with the table about the state of every node using the same signal space
  * Worker is responsible for processing input for some activity
  */
class CliftonNode extends Thread {

  val nodeId: String = UUID.randomUUID().toString

  private val signalSpace = SpaceCache.getSignalSpace
  private val dataSpace = SpaceCache.getDataSpace

  //templates to search in spaces
  private var templateAct = ExoEntry("", null)
  private val templateTable = ExoEntry(TABLE_MARKER, null)
  private var templateData: DataEntry = DataEntry(null, null, null, null)
  private var templateUpdateAct = ExoEntry(INFO_MARKER, (nodeId, UNDEFINED_ACT_ID))
  private val templateMySignals = ExoEntry(nodeId, null)
  private val templateNodeSignals = ExoEntry(NODE_SIGNAL_MARKER, null)

  /**
    * Update the space with the current information
    */
  private def updateNodeInfo(): Unit = {
    signalSpace.write(templateUpdateAct, NODE_INFO_LEASE_TIME)
  }

  private def tableHasAnalyser(tab: TableType): Boolean = {
    tab.get(ANALYSER_ACT_ID) match {
      case Some(analyserCount) => analyserCount != 0
      case None => true
    }
  }

  private def filterActivities(table: TableType): TableType =
    table.filterNot { case (id, _) => id == ANALYSER_ACT_ID || id == UNDEFINED_ACT_ID }

  val waitQueue: BlockingQueue[Unit] = new LinkedBlockingQueue[Unit]()

  def finishedProcessing(): Unit = waitQueue.add(())

  override def run(): Unit = {

    //current worker definitions
    var worker: WorkType = NoWork
    var processing: Option[Thread with BusyWorking] = None

    //times initializer
    var sleepTime = NODE_MIN_SLEEP_TIME
    var checkTime = System.currentTimeMillis()
    var killWhenIdle = false

    def nodeFullId: String = {
      val fromId = if (worker.hasWork)
        worker.activity.id
      else
        processing match {
          case Some(_: Analyser) => ANALYSER_ACT_ID
          case _ => UNDEFINED_ACT_ID
        }
      s"$nodeId($fromId)"
    }

    val bootMessage = s"Node $nodeFullId is ready to start"
    println(bootMessage)
    Log.info(bootMessage)
    while (true) {
      handleSignals()

      if (killWhenIdle) {
        killOwnSignal()
      }

      worker match {
        // worker is not defined yet
        case NoWork =>
          doNoWork()
        case joinWork@JoinWork(activity, _) =>
          if (!tryToDoJoin(joinWork))
            sleepForAWhile()

          //checks if it needs to change mode
          checkNeedToChange(activity.id)
        case consWork@ConsecutiveWork(activity) =>
          if (!consecutiveWork(consWork))
            sleepForAWhile()

          //checks if it needs to change mode
          checkNeedToChange(activity.id)
      }
    }

    def sleepForAWhile(): Unit = {
      // if nothing was found, it will sleep for a while
      Thread.sleep(sleepTime)
      sleepTime = math.min(sleepTime + NODE_STEP_SLEEP_TIME, NODE_MAX_SLEEP_TIME)
    }

    def doNoWork(): Unit = {
      signalSpace.read(templateTable, ENTRY_READ_TIME) match {
        case Some(tableEntry) =>
          val table = tableEntry.payload.asInstanceOf[TableType]
          if (!tryToBeAnalyser(table)) {
            sleepTime = NODE_MIN_SLEEP_TIME
            getRandomActivity(filterActivities(table)).foreach(act => setActivity(act))
            updateNodeInfo()
          }
        case None =>
          sleepForAWhile()
      }
    }

    def consecutiveWork(consWork: ConsecutiveWork): Boolean = {
      //get something to process
      dataSpace.take(templateData, ENTRY_READ_TIME) match {
        case Some(dataEntry) =>
          //if something was found
          sleepTime = NODE_MIN_SLEEP_TIME
          process(Vector(dataEntry), consWork.activity)
          true
        case None =>
          false
      }
    }

    /**
      * Receives the tableEntry and checks if the position where analyser node is defined has zero
      * If true, tries to take the table from the space and if it gets it, starts the analyser mode
      * Once analyser mode is started the table is updated in space with the info that already exists
      * one clifton node in analyser mode
      *
      * @return true if the node was transformed in analyser node, false otherwise
      */
    def tryToBeAnalyser(originalTable: TableType): Boolean = {
      if (!tableHasAnalyser(originalTable)) {
        signalSpace.take(templateTable, ENTRY_READ_TIME).map {
          tableEntry =>
            val table = tableEntry.payload.asInstanceOf[TableType]
            if (!tableHasAnalyser(table)) {
              val analyserThread = new AnalyserThread(nodeId, table)
              analyserThread.start()

              val bootMessage = s"Node $nodeFullId changed to Analyser mode"
              println(bootMessage)
              Log.info(bootMessage)

              processing.foreach { thread =>
                thread.interrupt()
                thread.join()
              }
              processing = Some(analyserThread)
              while (true) {
                handleSignals()
                Thread.sleep(ANALYSER_SLEEP_TIME)
              }
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
      signalSpace.take(templateMySignals, ENTRY_READ_TIME).foreach {
        mySignalEntry =>
          val mySignal = mySignalEntry.payload.asInstanceOf[NodeSignal]
          processSignal(mySignal)
      }
      signalSpace.take(templateNodeSignals, ENTRY_READ_TIME).foreach {
        nodeSignalEntry =>
          val nodeSignal = nodeSignalEntry.payload.asInstanceOf[NodeSignal]
          processSignal(nodeSignal)
      }
    }

    /**
      * kills the current node
      */
    def killOwnSignal(): Unit = {
      Log.info(s"Node $nodeFullId is going to shutdown")
      processing.foreach { thread =>
        thread.interrupt()
        thread.join()
      }

      //aborts the current node
      while (true) Thread.currentThread().interrupt()
    }

    /**
      * if its a KillSignal the node immediately aborts and dies, if its a KillGraceFullSignal
      * killWhenIdle is changed to true and before receiving something new to process the node dies
      *
      * @param nodeSignal
      */
    def processSignal(nodeSignal: NodeSignal) = {
      nodeSignal match {
        case KillSignal => killOwnSignal()
        case KillGracefullSignal =>
          killWhenIdle = true
      }
    }

    def tryToDoJoin(joinWork: JoinWork): Boolean = {
      if (tryToReadAll(Random.shuffle(joinWork.actsFrom))) {
        tryToTakeAll(joinWork.actsFrom) match {
          case Vector() =>
          // Other node was faster to take the data
          // Just restart the process
          case values: Vector[DataEntry] =>
            if (values.size == joinWork.actsFrom.size) {
              // we have all values, so we can continue
              sleepTime = NODE_MIN_SLEEP_TIME
              process(values, joinWork.activity)
            } else {
              // some values were lost ?
              Log.error(s"Data was missing from node $nodeFullId" +
                s" with injectId=${values.head.injectId}, ${values.size} values found, " +
                s"${joinWork.actsFrom.size} values expected")
            }
        }
        true
      } else
        false
    }

    /**
      * try to read all the results of an injectID that should do a Join
      *
      * @param actsFrom
      * @return true if both activities of the join are already present in the space
      */
    def tryToReadAll(actsFrom: Vector[String]): Boolean = {
      dataSpace.read(templateData.setFrom(actsFrom(0)).setInjectId(null), ENTRY_READ_TIME) match {
        case Some(dataEntry) =>
          templateData = templateData.setInjectId(dataEntry.injectId)
          for (act <- 1 until actsFrom.size) {
            if (dataSpace.read(templateData.setFrom(actsFrom(act)), ENTRY_READ_TIME).isEmpty)
              return false
          }
          true
        case None =>
          false
      }
    }

    /**
      * Try to read all the results of an injectID that should do a Join
      *
      * @param actsFrom
      * @return if both activities of the join was successfully taken from the space it returns
      *         a vector with them
      */
    def tryToTakeAll(actsFrom: Vector[String]): Vector[DataEntry] = {

      def tryToTakeAllAux(index: Int, acc: Vector[DataEntry]): Vector[DataEntry] = {
        if (index >= actsFrom.size) {
          acc
        } else {
          val from = actsFrom(index)
          dataSpace.take(templateData.setFrom(from), ENTRY_READ_TIME) match {
            case None => acc
            case Some(dataEntry) => tryToTakeAllAux(index + 1, acc :+ dataEntry)
          }
        }
      }

      tryToTakeAllAux(0, Vector())
    }

    /**
      * If a worker is already defined we send the input to be processed
      * else, a new thread is created and then the input is send
      *
      * @param dataEntry
      * @param activity
      */
    def process(dataEntry: Vector[DataEntry], activity: ActivityWorker): Unit = {
      val worker: Worker with BusyWorking =
        processing match {
          case Some(workerThread: Worker) =>
            workerThread
          case _ =>
            val workerThread = new WorkerThread(this)
            workerThread.start()
            processing = Some(workerThread)
            workerThread
        }
      worker.sendInput(activity, dataEntry)

      while (worker.threadIsBusy) {
        handleSignals()
        try {
          //        Thread.sleep(10000)
          waitQueue.poll(10000, TimeUnit.MILLISECONDS)
        } catch {
          case _: InterruptedException =>
          // thread finished processing
          // So, back to normal mode
        }
      }
    }

    /**
      * checks in a period of time if this node needs to change
      *
      * @param actId
      */
    def checkNeedToChange(actId: String): Unit = {
      val nowTime = System.currentTimeMillis()
      if (nowTime - checkTime > NODE_CHECK_TABLE_TIME) {
        signalSpace.read(templateTable, ENTRY_READ_TIME).foreach {
          tableEntry =>
            //first we need to check if the table already contains an analyser
            val table = tableEntry.payload.asInstanceOf[TableType]
            if (!tryToBeAnalyser(table)) {
              val filteredTable = filterActivities(table)
              if (filteredTable.size > 1) {
                val totalNodes = filteredTable.values.sum
                val n = 1.0 / filteredTable.size
                val q = filteredTable(actId).toDouble / totalNodes
                val uw = 1.0 / totalNodes
                //checks if its need to update function
                if (q > n && Random.nextDouble() < (q - n) / q) {
                  //should i transform
                  getRandomActivity(filteredTable).foreach {
                    newAct =>
                      val qNew = filteredTable(newAct).toDouble / totalNodes
                      if (newAct != actId && (q - uw >= n || qNew + uw <= n)) {
                        setActivity(newAct)
                      }
                  }
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
      * @return returns a random activity id from the table
      */
    def getRandomActivity(table: TableType): Option[String] = {
      val tableList: List[TableEntryType] = table.toList
      if (tableList.isEmpty)
        None
      else {
        val total = tableList.unzip._2.sum
        val n = 1.0 / tableList.size
        val list: List[TableEntryType] = tableList.filter(_._2.toDouble / total < n)

        if (list.isEmpty)
          Some(tableList(Random.nextInt(tableList.size))._1)
        else
          Some(list(Random.nextInt(list.size))._1)
      }
    }

    /**
      * sets the node to process input to a specific activity
      * updates the templates and worker
      *
      * @param activityId
      */
    def setActivity(activityId: String): Unit = {
      templateAct = templateAct.setMarker(activityId)
      signalSpace.read(templateAct, ENTRY_READ_TIME) match {
        case None =>
          Log.error(s"ActivitySignal for activity $activityId not found in SignalSpace")
          Thread.sleep(ACT_NOT_FOUND_SLEEP_TIME)
        case Some(entry) => entry.payload match {
          case activitySignal: ActivitySignal =>
            ActivityCache.getActivity(activitySignal.name) match {
              case Some(activity) =>
                templateUpdateAct = templateUpdateAct.setPayload((nodeId, activityId))
                templateData = DataEntry(activityId, null, null, null)
                val activityWorker = new ActivityWorker(activityId, activity, activitySignal.params, activitySignal.outMarkers)
                val msg = s"Node $nodeFullId changed to $activityId"
                activitySignal.inMarkers match {
                  case Vector(_) =>
                    worker = ConsecutiveWork(activityWorker)
                  case actFrom: Vector[String] =>
                    worker = JoinWork(activityWorker, actFrom)
                }
                Log.info(msg)
              case None =>
                Log.error("Class could not be loaded: " + activitySignal.name)
                Thread.sleep(ACT_NOT_FOUND_SLEEP_TIME)
            }
        }
      }
    }
  }

}
