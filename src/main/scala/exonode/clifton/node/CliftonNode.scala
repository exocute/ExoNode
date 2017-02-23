package exonode.clifton.node

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}
import java.util.{Date, UUID}

import exonode.clifton.config.BackupConfig
import exonode.clifton.config.Protocol._
import exonode.clifton.node.CliftonNode._
import exonode.clifton.node.Log.{INFO, ERROR, WARN, ND}
import exonode.clifton.node.entries.{DataEntry, ExoEntry}
import exonode.clifton.node.work.{ConsecutiveWork, _}
import exonode.clifton.signals._

import scala.util.Random

/**
  * Created by #ScalaTeam on 05-01-2017.
  *
  * Generic node. Nodes are responsible for processing activities or analysing INFOs from the space.
  * A CliftonNode has two main modes: Analyser or Worker
  * Analyser is responsible for updating the space with the table about the state of every node using the same signal space
  * Worker is responsible for processing input for some activity
  */
class CliftonNode(implicit backupConfig: BackupConfig) extends Thread with Node {

  val nodeId: String = UUID.randomUUID().toString

  private val signalSpace = SpaceCache.getSignalSpace
  private val dataSpace = SpaceCache.getDataSpace

  private class KillSignalException extends Exception

  private def debug(msg: String, writeToLog: Boolean = true) = {
    if (DEBUG) {
      println(new Date().toString, nodeId, msg)
      if (writeToLog)
        ???
      //Log.info(nodeId, msg)
    }
  }

  /**
    * Filter the undefined activity from the table
    *
    * @param table the table to be filtered
    * @return the filtered table
    */
  private def filterActivities(table: TableType): TableType =
    table.filterNot { case (id, _) => id == UNDEFINED_ACT_ID }

  val waitQueue: BlockingQueue[Unit] = new LinkedBlockingQueue[Unit]()

  def finishedProcessing(): Unit = waitQueue.add(())

  override def run(): Unit = {

    //templates to search in spaces
    var templateAct = ExoEntry("", null)
    val templateTable = ExoEntry(TABLE_MARKER, null)
    var templateData: DataEntry = DataEntry(null, null, null, null)
    var templateUpdateAct = ExoEntry(INFO_MARKER, (nodeId, UNDEFINED_ACT_ID, NOT_PROCESSING_MARKER))
    val templateMySignals = ExoEntry(nodeId, null)
    val templateNodeSignals = ExoEntry(NODE_SIGNAL_MARKER, null)
    val templateWantToBeAnalyser = ExoEntry(WANT_TO_BE_ANALYSER_MARKER, null)

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
          case Some(_: Analyser) => ANALYSER_MARKER
          case _ => UNDEFINED_ACT_ID
        }
      s"$nodeId($fromId)"
    }

    val bootMessage = s"Node is ready to start"
    println(s"$nodeFullId;$bootMessage")
    Log.receiveLog(LoggingSignal(STARTED_NODE, INFO, nodeId, ND, ND, UNDEFINED_ACT_ID, ND, bootMessage, 0))
    try {
      while (true) {
        handleSignals()

        if (killWhenIdle)
          killOwnSignal()

        worker match {
          case NoWork =>
            // worker is not defined yet
            doNoWork()
          case w if w.hasWork =>
            searchForDataToProcess()
            if (!checkNeedToChange(worker.activity.id)) {
              updateNodeInfo()
              sleepForAWhile()
            }
        }
      }
    } catch {
      case _: KillSignalException => println(s"Going to stop: $nodeFullId")
    }

    def searchForDataToProcess(): Unit = {
      worker match {
        case JoinWork(activity, _) =>
          if (tryToDoJoin(worker.asInstanceOf[JoinWork])) {
            //checks if it needs to change mode
            checkNeedToChange(activity.id)
            searchForDataToProcess()
          }
        case ConsecutiveWork(activity) =>
          if (consecutiveWork(worker.asInstanceOf[ConsecutiveWork])) {
            //checks if it needs to change mode
            checkNeedToChange(activity.id)
            searchForDataToProcess()
          }
        case _ => // ignore
      }
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
          val filteredTable = filterActivities(table)
          if (filteredTable.isEmpty) {
            updateNodeInfo()
            sleepForAWhile()
          } else {
            sleepTime = NODE_MIN_SLEEP_TIME
            getRandomActivity(filteredTable).foreach(act => setActivity(act))
          }
        case None =>
          consensusAnalyser()
          sleepForAWhile()
      }
    }

    def consensusAnalyser(): Unit = {
      Thread.sleep(consensusRandomSleepTime())
      auxConsensusAnalyser()
      Thread.sleep(CONSENSUS_MAX_SLEEP_TIME)

      def auxConsensusAnalyser(loopNumber: Int = 0): Unit = {
        signalSpace.readMany(templateWantToBeAnalyser, CONSENSUS_ENTRIES_TO_READ).toList match {
          case Nil =>
            if (signalSpace.read(templateTable, ENTRY_READ_TIME).isEmpty) {
              signalSpace.write(templateWantToBeAnalyser.setPayload(nodeId), CONSENSUS_WANT_TBA_LEASE_TIME)
              debug("Wants to be analyser")
              Thread.sleep(consensusRandomSleepTime())
              auxConsensusAnalyser()
            }
          case List(entry) =>
            if (loopNumber >= CONSENSUS_LOOPS_TO_FINISH) {
              if (entry.payload == nodeId) {
                // Test if really there isn't a table (and therefore an analyser)
                //FIXME 500 => magic number
                if (signalSpace.read(templateTable, 500).isEmpty) {
                  signalSpace.write(ExoEntry(TABLE_MARKER, EMPTY_TABLE), TABLE_LEASE_TIME)
                  if (signalSpace.take(entry, ENTRY_READ_TIME).isDefined) {
                    // Everything worked fine and the consensus is successful
                    debug("found that consensus is successful")
                    transformIntoAnalyser()
                  } else {
                    // The consensus have failed (want-to-be-analyser entry have timeout from the space?)
                    debug("found that consensus have failed")
                    signalSpace.take(TABLE_MARKER, ENTRY_READ_TIME)
                  }
                } else {
                  debug("found a table when the node was going to be an analyser")
                }
              }
            }
            else {
              debug(s"is going to loop ${loopNumber + 1}")
              Thread.sleep(CONSENSUS_MAX_SLEEP_TIME)
              auxConsensusAnalyser(loopNumber + 1)
            }
          case entries: List[ExoEntry] =>
            val maxId = entries.maxBy(_.payload.toString).payload
            for {
              entry <- entries
              if entry.payload != maxId
            } {
              debug(s"is deleting entry with id ${entry.payload}")
              signalSpace.take(entry, ENTRY_READ_TIME)
            }
            Thread.sleep(consensusRandomSleepTime())
            auxConsensusAnalyser()
        }
      }
    }

    def transformIntoAnalyser(): Unit = {
      processing.foreach { thread =>
        thread.interrupt()
        thread.join()
      }

      val analyserThread = new AnalyserThread(nodeId)
      processing = Some(analyserThread)
      analyserThread.start()

      val analyserBootMessage = "Node changed to analyser mode"
      println(s"$nodeFullId;$analyserBootMessage")
      Log.receiveLog(LoggingSignal(CHANGED_ACT, INFO, nodeId, ND, UNDEFINED_ACT_ID, ANALYSER_MARKER, ND, analyserBootMessage, 0))

      while (true) {
        handleSignals()
        Thread.sleep(ANALYSER_SLEEP_TIME)
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
          dataSpace.write(dataEntry.createBackup(), backupConfig.BACKUP_DATA_LEASE_TIME)
          dataSpace.write(dataEntry.createInfoBackup(), backupConfig.BACKUP_DATA_LEASE_TIME)
          Some(dataEntry)
        case None => None
      }
    }

    /**
      * Reads signals from the space that be general to every node or specific to some node with
      * a defined ID
      */
    def handleSignals(): Unit = {
      signalSpace.take(templateMySignals, ENTRY_READ_TIME).foreach {
        mySignalEntry =>
          debug(s"Reading a signal: $mySignalEntry")
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
      val shutdownMsg = "Node is going to shutdown"
      println(s"$nodeFullId;$shutdownMsg")
      Log.receiveLog(LoggingSignal(NODE_SHUTDOWN, INFO, nodeId, ND, ND, ND, ND, s"$nodeFullId;$shutdownMsg", 0))
      processing match {
        case None =>
        case Some(analyserThread: Analyser) =>
          analyserThread.cancelThread()
          analyserThread.join()
        case Some(thread) =>
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
              val msg = s"Data was missing with injectId=${values.head.injectId}, " +
                s"${values.size} values found, ${joinWork.actsFrom.size} values expected"
              println(nodeFullId + ";" + msg)
              Log.receiveLog(LoggingSignal(VALUES_LOST, ERROR, nodeId, ND, ND, ND, ND, msg, 0))
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
            val entry = dataSpace.read(templateData.setFrom(actsFrom(act)), ENTRY_READ_TIME)
            if (entry.isEmpty)
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

    def renewBackup(dataEntries: Vector[DataEntry]): Unit = {
      for (dataEntry <- dataEntries) {
        dataSpace.write(dataEntry.createBackup(), backupConfig.BACKUP_DATA_LEASE_TIME)
        dataSpace.write(dataEntry.createInfoBackup(), backupConfig.BACKUP_DATA_LEASE_TIME)
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
      val workerThread: WorkerThread =
        processing match {
          case Some(workerThread: WorkerThread) =>
            workerThread
          case _ =>
            val workerThread = new WorkerThread(this)
            processing = Some(workerThread)
            workerThread.start()
            workerThread
        }
      workerThread.sendInput(activity, dataEntries)
      templateUpdateAct = templateUpdateAct.setPayload(nodeId, activity.id, dataEntries.head.injectId)

      var initProcessTime = System.currentTimeMillis()
      var initDataProcessTime = System.currentTimeMillis()

      while (workerThread.threadIsBusy) {
        handleSignals()
        val currentTime = System.currentTimeMillis()
        if (currentTime - initProcessTime > backupConfig.SEND_STILL_PROCESSING_TIME) {
          initProcessTime = currentTime
          updateNodeInfo(force = true)
        }
        if (currentTime - initDataProcessTime > backupConfig.RENEW_BACKUP_ENTRIES_TIME) {
          initDataProcessTime = currentTime
          renewBackup(dataEntries)
        }
        try {
          waitQueue.poll(10000, TimeUnit.MILLISECONDS)
        } catch {
          case _: InterruptedException =>
          // thread finished processing
          // So, back to normal mode
        }
      }
      templateUpdateAct = templateUpdateAct.setPayload(nodeId, activity.id, NOT_PROCESSING_MARKER)
    }

    /**
      * checks in a period of time if this node needs to change
      *
      * @param actId
      */
    def checkNeedToChange(actId: String): Boolean = {
      val nowTime = System.currentTimeMillis()
      if (nowTime - checkTime > NODE_CHECK_TABLE_TIME) {
        checkTime = nowTime
        signalSpace.read(templateTable, ENTRY_READ_TIME) match {
          case None => consensusAnalyser()
          case Some(tableEntry) =>
            val table = tableEntry.payload.asInstanceOf[TableType]
            val filteredTable = filterActivities(table)

            //Test if the table still contains the current activity
            if (!filteredTable.contains(actId)) {
              getRandomActivity(filteredTable) match {
                case None => setNeutralMode()
                case Some(act) =>
                  setActivity(act)
                  return true
              }
            } else {
              // there needs to be at least one activity to jump to
              if (filteredTable.size > 1) {
                val totalNodes = filteredTable.values.sum
                val n = 1.0 / filteredTable.size
                val q = filteredTable(actId).toDouble / totalNodes
                val uw = 1.0 / totalNodes
                //checks if its need to update function
                if (q > n && Random.nextDouble() < (q - n) / q) {
                  //should i transform
                  getRandomActivity(filteredTable) match {
                    case None => setNeutralMode()
                    case Some(newAct) =>
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
        val total = math.max(1, tableList.unzip._2.sum)
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
          println(s"$nodeFullId;ActivitySignal for activity $activityId not found in SignalSpace")
          Log.receiveLog(LoggingSignal(ACTIVITY_NOT_FOUND, WARN, nodeId, ND, activityId, ND, ND, s"$nodeFullId;ActivitySignal for activity $activityId not found in SignalSpace", 0))
          Thread.sleep(ERROR_SLEEP_TIME)
        case Some(entry) => entry.payload match {
          case activitySignal: ActivitySignal =>
            ActivityCache.getActivity(activitySignal.name) match {
              case Some(activity) =>
                templateUpdateAct = templateUpdateAct.setPayload((nodeId, activityId, NOT_PROCESSING_MARKER))
                templateData = DataEntry(activityId, null, null, null)
                val activityWorker = new ActivityWorker(activityId, activity, activitySignal.params, activitySignal.outMarkers)
                if (worker.hasWork)
                  Log.receiveLog(LoggingSignal(CHANGED_ACT, INFO, nodeId, ND, worker.activity.id, activityId, ND, s"Node changed to $activityId", 0))
                else Log.receiveLog(LoggingSignal(CHANGED_ACT, INFO, nodeId, ND, UNDEFINED_ACT_ID, activityId, ND, s"Node changed to $activityId", 0))
                activitySignal.inMarkers match {
                  case Vector(_) =>
                    worker = ConsecutiveWork(activityWorker)
                  case actsFrom: Vector[String] =>
                    worker = JoinWork(activityWorker, actsFrom)
                }
                // send the updated information
                updateNodeInfo(force = true)
                sleepTime = NODE_MIN_SLEEP_TIME
              case None =>
                println(nodeFullId + ";Class could not be loaded: " + activitySignal.name)
                Log.receiveLog(LoggingSignal(CLASS_NOT_LOADED, WARN, nodeId, ND, ND, ND, ND, "Class could not be loaded: " + activitySignal.name, 0))
                Thread.sleep(ERROR_SLEEP_TIME)
            }
        }
      }
    }

    def setNeutralMode(): Unit = {
      worker = NoWork
      templateUpdateAct = templateUpdateAct.setPayload((nodeId, UNDEFINED_ACT_ID, NOT_PROCESSING_MARKER))
      updateNodeInfo(force = true)
    }
  }

}

object CliftonNode {
  var DEBUG: Boolean = false
}