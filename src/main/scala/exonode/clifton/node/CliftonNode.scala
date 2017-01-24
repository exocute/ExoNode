package exonode.clifton.node

import java.util.UUID
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}

import exonode.clifton.Protocol._
import exonode.clifton.node.entries.{BackupEntry, DataEntry, ExoEntry}
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

  private class KillSignalException extends Exception

  private def tableHasAnalyser(tab: TableType): Boolean = {
    tab.get(ANALYSER_ACT_ID) match {
      case Some(analyserCount) => analyserCount != 0
      case None => false
    }
  }

  private def filterActivities(table: TableType): TableType =
    table.filterNot { case (id, _) => id == ANALYSER_ACT_ID || id == UNDEFINED_ACT_ID }

  val waitQueue: BlockingQueue[Unit] = new LinkedBlockingQueue[Unit]()

  def finishedProcessing(): Unit = waitQueue.add(())

  override def run(): Unit = {

    //templates to search in spaces
    var templateAct = ExoEntry("", null)
    val templateTable = ExoEntry(TABLE_MARKER, null)
    var templateData: DataEntry = DataEntry(null, null, null, null)
    var templateUpdateAct = ExoEntry(INFO_MARKER, (nodeId, UNDEFINED_ACT_ID, NOT_PROCESSING))
    val templateMySignals = ExoEntry(nodeId, null)
    val templateNodeSignals = ExoEntry(NODE_SIGNAL_MARKER, null)

    //current worker definitions
    var worker: WorkType = NoWork
    var processing: Option[Thread with BusyWorking] = None

    //times initializer
    var sleepTime = NODE_MIN_SLEEP_TIME
    var checkTime = System.currentTimeMillis()
    var sendInfoTime = 0L
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
    try {
      while (true) {
        handleSignals()

        if (killWhenIdle)
          killOwnSignal()

        worker match {
          case NoWork =>
            // worker is not defined yet
            doNoWork()
          case work if work.hasWork =>
            work match {
              case joinWork@JoinWork(activity, _) =>
                while (tryToDoJoin(joinWork)) {
                  //checks if it needs to change mode
                  checkNeedToChange(activity.id)
                }
              case consWork@ConsecutiveWork(activity) =>
                while (consecutiveWork(consWork)) {
                  //checks if it needs to change mode
                  checkNeedToChange(activity.id)
                }
              case _ => // ignore
            }
            if (!checkNeedToChange(work.activity.id)) {
              updateNodeInfo()
              sleepForAWhile()
            }
        }
      }
    } catch {
      case _: KillSignalException => println(s"Going to stop: $nodeFullId")
    }

    /**
      * Update the space with the current information
      */
    def updateNodeInfo(force: Boolean = false): Unit = {
      val currentTime = System.currentTimeMillis()
      if (force || currentTime - sendInfoTime > NODE_CHECK_TABLE_TIME) {
        signalSpace.write(templateUpdateAct, NODE_INFO_LEASE_TIME)
        sendInfoTime = currentTime
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
            val filteredTable = filterActivities(table)
            if (filteredTable.isEmpty) {
              updateNodeInfo()
              sleepForAWhile()
            } else {
              getRandomActivity(filteredTable).foreach(act => setActivity(act))
            }
          }
        case None =>
          sleepForAWhile()
      }
    }

    def consecutiveWork(consWork: ConsecutiveWork): Boolean = {
      //get something to process
      takeAndBackup(templateData) match {
        case Some(dataEntry) =>
          //if something was found
          sleepTime = NODE_MIN_SLEEP_TIME
          process(Vector(dataEntry), consWork.activity)
          true
        case None =>
          false
      }
    }

    def takeAndBackup(tempData: DataEntry): Option[DataEntry] = {
      dataSpace.take(tempData, ENTRY_READ_TIME) match {
        case Some(dataEntry) =>
          dataSpace.write(dataEntry.createBackup(), BACKUP_LEASE_TIME)
          dataSpace.write(dataEntry.createInfoBackup(), BACKUP_LEASE_TIME)
          Some(dataEntry)
        case None => None

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
            if (tableHasAnalyser(table)) {
              // other node already is an analyser, so just write the table back
              signalSpace.write(tableEntry, TABLE_LEASE_TIME)
            } else {
              processing.foreach { thread =>
                thread.interrupt()
                thread.join()
              }

              val analyserThread = new AnalyserThread(nodeId, table)
              processing = Some(analyserThread)
              analyserThread.start()

              val bootMessage = s"Node $nodeFullId changed to Analyser mode"
              println(bootMessage)
              Log.info(bootMessage)

              while (true) {
                handleSignals()
                Thread.sleep(ANALYSER_SLEEP_TIME)
              }
              return true
            }
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
      if (!killWhenIdle) {
        signalSpace.take(templateNodeSignals, ENTRY_READ_TIME).foreach {
          nodeSignalEntry =>
            val nodeSignal = nodeSignalEntry.payload.asInstanceOf[NodeSignal]
            processSignal(nodeSignal)
        }
      }
    }

    /**
      * kills the current node
      */
    def killOwnSignal(): Unit = {
      val shutdownMsg = s"Node $nodeFullId is going to shutdown"
      println(shutdownMsg)
      Log.info(shutdownMsg)
      processing.foreach { thread =>
        thread.interrupt()
        thread.join()
      }
      throw new KillSignalException
    }

    /**
      * if its a KillSignal the node immediately aborts and dies, if its a KillGraceFullSignal
      * killWhenIdle is changed to true and before receiving something new to process the node dies
      *
      * @param nodeSignal
      */
    def processSignal(nodeSignal: NodeSignal): Unit = {
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
          takeAndBackup(templateData.setFrom(from)) match {
            case None => acc
            case Some(dataEntry) => tryToTakeAllAux(index + 1, acc :+ dataEntry)
          }
        }
      }

      tryToTakeAllAux(0, Vector())
    }

    def renewerBackup(dataEntries: Vector[DataEntry]) = {
      for (dataEntry <- dataEntries) {
        dataSpace.write(dataEntry.createBackup(), BACKUP_LEASE_TIME)
        dataSpace.write(dataEntry.createInfoBackup(), BACKUP_LEASE_TIME)
      }
    }

    /**
      * If a worker is already defined we send the input to be processed
      * else, a new thread is created and then the input is send
      *
      * @param dataEntries
      * @param activity
      */
    def process(dataEntries: Vector[DataEntry], activity: ActivityWorker): Unit = {
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
      worker.sendInput(activity, dataEntries)
      templateUpdateAct = templateUpdateAct.setPayload(nodeId, activity.id, dataEntries.head.injectId)

      var initProcessTime = System.currentTimeMillis()
      var initDataProcessTime = System.currentTimeMillis()

      while (worker.threadIsBusy) {
        handleSignals()
        val currentTime = System.currentTimeMillis()
        if (currentTime - initProcessTime > BACKUP_UPDATE_INFO_TIME) {
          initProcessTime = currentTime
          println("Renew!")
          updateNodeInfo(true)
        }
        if (currentTime - initDataProcessTime > BACKUP_UPDATE_DATA_TIME) {
          initDataProcessTime = currentTime
          renewerBackup(dataEntries)
        }
        try {
          //        Thread.sleep(10000)
          waitQueue.poll(100, TimeUnit.MILLISECONDS)
        } catch {
          case _: InterruptedException =>
          // thread finished processing
          // So, back to normal mode
        }
      }
      templateUpdateAct = templateUpdateAct.setPayload(nodeId, activity.id, NOT_PROCESSING)
    }

    /**
      * checks in a period of time if this node needs to change
      *
      * @param actId
      */
    def checkNeedToChange(actId: String): Boolean = {
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
                      println(s"$nodeFullId trying to change to $newAct    ($filteredTable)")

                      val qNew = filteredTable(newAct).toDouble / totalNodes
                      if (q - uw >= n || qNew + uw <= n) {
                        setActivity(newAct)
                        return true
                      }
                  }
                }
              }
            }
        }
        checkTime = nowTime
      }
      false
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
        // excludes the current activity and others that probably don't need more nodes
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
                templateUpdateAct = templateUpdateAct.setPayload((nodeId, activityId, NOT_PROCESSING))
                templateData = DataEntry(activityId, null, null, null)
                val activityWorker = new ActivityWorker(activityId, activity, activitySignal.params, activitySignal.outMarkers)
                Log.info(s"Node $nodeFullId changed to $activityId")
                activitySignal.inMarkers match {
                  case Vector(_) =>
                    worker = ConsecutiveWork(activityWorker)
                  case actFrom: Vector[String] =>
                    worker = JoinWork(activityWorker, actFrom)
                }
                // send the updated information
                updateNodeInfo(force = true)
                sleepTime = NODE_MIN_SLEEP_TIME
              case None =>
                Log.error("Class could not be loaded: " + activitySignal.name)
                Thread.sleep(ACT_NOT_FOUND_SLEEP_TIME)
            }
        }
      }
    }
  }

}
