package exonode.clifton.node

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}
import java.util.{Date, UUID}

import exonode.clifton.config.ProtocolConfig
import exonode.clifton.config.ProtocolConfig._
import exonode.clifton.node.entries.{DataEntry, ExoEntry, GraphEntry}
import exonode.clifton.node.work._
import exonode.clifton.signals.Log._
import exonode.clifton.signals.Log.Log
import exonode.clifton.signals._

import scala.util.Random

/**
  * Created by #GrowinScala
  *
  * Generic node. Nodes are responsible for processing activities or analysing INFOs from the space.
  * A CliftonNode has two main modes: Analyser or Worker
  * Analyser is responsible for updating the space with the table about the state of every node using the same signal space
  * Worker is responsible for processing input for some activity
  */
class CliftonNode extends Thread with Node {

  val nodeId: String = UUID.randomUUID().toString

  private val signalSpace = SpaceCache.getSignalSpace
  private val dataSpace = SpaceCache.getDataSpace

  private class KillSignalException extends Exception

  val waitQueue: BlockingQueue[Unit] = new LinkedBlockingQueue[Unit]()
  private var config: ProtocolConfig = ProtocolConfig.Default

  @inline private def debug(msg: => String) = {
    CliftonNode.debug(nodeId, msg)
  }

  /**
    * Filter the undefined activities from the table
    *
    * @param table the table to be filtered
    * @return the filtered table
    */
  private def filterActivities(table: TableType): TableType =
    table.filterNot { case (id, _) => id == UndefinedActId }

  def finishedProcessing(): Unit = waitQueue.add(())

  def updateConfig(newConfig: ProtocolConfig): Unit = {
    if (config.Id != newConfig.Id)
      config = newConfig
  }

  /**
    * Starting point of a CliftonNode
    */
  override def run(): Unit = {
    //templates to search in spaces
    val templateAct = ExoEntry[ActivitySignal]("", null)
    val templateTable = ExoEntry[AnalyserTable](TableMarker, null)
    var templateData: DataEntry = DataEntry(null, null, null, null, null)
    var templateUpdateAct = ExoEntry[NodeInfoType](InfoMarker, NodeInfoType(nodeId, UndefinedActId, None))
    val templateMySignals = ExoEntry[NodeSignal](nodeId, null)
    val templateNodeSignals = ExoEntry[NodeSignal](NodeSignalMarker, null)
    val templateWantAnalyser = ExoEntry[String](WantToBeAnalyserMarker, null)

    //current worker definitions
    var worker: WorkType = NoWork
    var processing: Option[Thread with BusyWorking] = None

    //times initializer
    var sleepTime = config.NodeMinSleepTime
    var checkTime = System.currentTimeMillis()
    var sendInfoTime = 0L
    var killWhenIdle = false

    def fromId: String =
      if (worker.hasWork)
        worker.activity.id
      else
        processing match {
          case Some(_: Analyser) => AnalyserMarker
          case _ => UndefinedActId
        }

    def nodeFullId: String = {
      s"$nodeId($fromId)"
    }

    val bootMessage = s"Node started"
    println(s"$nodeFullId;$bootMessage")
    Log.writeLog(LogStartedNode(nodeId))
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
        case w @ JoinWork(activity, _) =>
          if (tryToDoJoin(w)) {
            //checks if it needs to change mode
            checkNeedToChange(activity.id)
            searchForDataToProcess()
          }
        case w @ ConsecutiveWork(activity) =>
          if (consecutiveWork(w)) {
            //checks if it needs to change mode
            checkNeedToChange(activity.id)
            searchForDataToProcess()
          }
        case _ => // ignore
      }
    }

    /**
      * Updates the space with the current information
      *
      * @param force If it's true then the signal will be sent
      *              regardless of the last sendInfoTime.
      *              If false (default) it only sends if some
      *              time has passed after the last update
      *              (defined by the NodeCheckTableTime in [[exonode.clifton.config.ProtocolConfig ProtocolConfig]])
      */
    def updateNodeInfo(force: Boolean = false): Unit = {
      val currentTime = System.currentTimeMillis()
      if (force || currentTime - sendInfoTime > config.NodeCheckTableTime) {
        signalSpace.write(templateUpdateAct, config.NodeInfoLeaseTime)
        sendInfoTime = currentTime
      }
    }

    def sleepForAWhile(): Unit = {
      // if nothing was found, it will sleep for a while
      Thread.sleep(sleepTime)
      sleepTime = math.min(sleepTime * 2, config.NodeMaxSleepTime)
    }

    def doNoWork(): Unit = {
      signalSpace.read(templateTable, 0) match {
        case Some(ExoEntry(_, AnalyserTable(table, analyserConfig))) =>
          updateConfig(analyserConfig)
          val filteredTable = filterActivities(table)
          if (filteredTable.isEmpty) {
            updateNodeInfo()
            sleepForAWhile()
          } else {
            sleepTime = config.NodeMinSleepTime
            getRandomActivity(filteredTable).foreach(act => setActivity(act))
          }
        case None =>
          consensusAnalyser()
          sleepForAWhile()
      }
    }

    /**
      * Starts the consensus algorithm to determine which node will be the next Analyser
      */
    def consensusAnalyser(): Unit = {
      Thread.sleep(config.consensusRandomSleepTime())
      auxConsensusAnalyser()
      Thread.sleep(config.ConsensusMaxSleepTime)

      def auxConsensusAnalyser(loopNumber: Int = 0): Unit = {
        signalSpace.readMany(templateWantAnalyser, config.ConsensusEntriesToRead).toList match {
          case Nil =>
            if (signalSpace.read(templateTable, 0).isEmpty) {
              signalSpace.write(templateWantAnalyser.setPayload(nodeId), config.ConsensusWantAnalyserTime)
              debug("Wants to be analyser")
              Thread.sleep(config.consensusRandomSleepTime())
              auxConsensusAnalyser()
            }
          case List(entry) =>
            if (loopNumber >= config.ConsensusLoopsToFinish) {
              if (entry.payload == nodeId) {
                // Test if really there isn't a table (and therefore an analyser)
                if (signalSpace.read(templateTable, config.ConsensusTestTableExistTime).isEmpty) {
                  signalSpace.write(ExoEntry(TableMarker, AnalyserTable(EmptyTable, config)), config.TableLeaseTime)
                  if (signalSpace.take(entry, 0).isDefined) {
                    // Everything worked fine and the consensus is successful
                    debug("found that consensus was successful")
                    transformIntoAnalyser()
                  } else {
                    // The consensus have failed (want-to-be-analyser entry have timeout from the space?)
                    debug("found that consensus has failed")
                    signalSpace.take(TableMarker, 0)
                  }
                } else {
                  debug("found a table when it was trying to be an analyser")
                }
              }
            }
            else {
              debug(s"is going to loop ${loopNumber + 1}")
              Thread.sleep(config.ConsensusMaxSleepTime)
              auxConsensusAnalyser(loopNumber + 1)
            }
          case entries: List[ExoEntry[_]] =>
            val maxId = entries.maxBy(_.payload.toString).payload
            for {
              entry <- entries
              if entry.payload != maxId
            } {
              debug(s"is deleting entry with id ${entry.payload}")
              signalSpace.take(entry, 0)
            }
            Thread.sleep(config.consensusRandomSleepTime())
            auxConsensusAnalyser()
        }
      }
    }

    /**
      * Transforms this node into an [[exonode.clifton.node.work.AnalyserThread Analyser]]
      */
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
      Log.writeLog(LogChangedAct(nodeId, fromId, AnalyserMarker, analyserBootMessage))

      while (true) {
        handleSignals()
        Thread.sleep(config.AnalyserSleepTime)
      }
    }

    def consecutiveWork(consWork: ConsecutiveWork): Boolean = {
      //get something to process
      takeAndBackup(templateData) match {
        case Some(dataEntry) =>
          //if something was found
          sleepTime = config.NodeMinSleepTime
          process(Vector(dataEntry), consWork.activity)
          true
        case None =>
          false
      }
    }

    /**
      * Tries to take a [[exonode.clifton.node.entries.DataEntry DataEntry]] from the space and
      * writes a backup of it.
      */
    def takeAndBackup(tempData: DataEntry): Option[DataEntry] = {
      dataSpace.take(tempData, 0) match {
        case Some(dataEntry) =>
          dataSpace.write(dataEntry.createBackup(), config.BackupDataLeaseTime)
          dataSpace.write(dataEntry.createInfoBackup(), config.BackupDataLeaseTime)
          Some(dataEntry)
        case None => None
      }
    }

    /**
      * Reads signals from the space that be generic to every node or specific to this node.
      */
    def handleSignals(): Unit = {
      signalSpace.take(templateMySignals, 0).foreach {
        case ExoEntry(_, mySignal) => processSignal(mySignal)
      }
      if (!killWhenIdle) {
        signalSpace.take(templateNodeSignals, 0).foreach {
          case ExoEntry(_, nodeSignal) => processSignal(nodeSignal)
        }
      }
    }

    /**
      * Kills this node.
      */
    def killOwnSignal(): Unit = {
      val shutdownMsg = "Node is going to shutdown"
      println(s"$nodeFullId;$shutdownMsg")
      Log.writeLog(LogNodeShutdown(nodeId, shutdownMsg))
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
      * If it's a KillSignal the node immediately aborts and dies.
      * If it's a KillGraceFullSignal then killWhenIdle is set to true,
      * so that before receiving something new to process the node will die.
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
              sleepTime = config.NodeMinSleepTime
              process(values, joinWork.activity)
            } else {
              // some values were lost ?
              val msg = s"Data was missing with injectId=${values.head.injectId}, " +
                s"${values.size} values found, ${joinWork.actsFrom.size} values expected"
              println(nodeFullId + ";" + msg)
              Log.writeLog(LogValuesLost(nodeId, msg))
            }
        }
        true
      } else
        false
    }

    /**
      * Tries to read all the results of an injectId that should do a join operation
      *
      * @return true if all activities of the join are already present in the space
      */
    def tryToReadAll(actsFrom: Vector[String]): Boolean = {
      dataSpace.read(templateData.setFrom(actsFrom(0)).setInjectId(null), 0) match {
        case Some(dataEntry) =>
          templateData = templateData.setInjectId(dataEntry.injectId)
          //          for (act <- 1 until actsFrom.size) {
          //            val entry = dataSpace.read(templateData.setFrom(actsFrom(act)), 0)
          //            if (entry.isEmpty)
          //              return false
          //          }
          (1 until actsFrom.size).forall {
            act =>
              val entry = dataSpace.read(templateData.setFrom(actsFrom(act)), 0)
              entry.isDefined
          }
        case None =>
          false
      }
    }

    /**
      * Tries to take all the results of an injectId that should do a join operation
      *
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

    /**
      * Renew the backups of the dataEntries this node is processing at the moment.
      */
    def renewBackup(dataEntries: Vector[DataEntry]): Unit = {
      for (dataEntry <- dataEntries) {
        dataSpace.write(dataEntry.createBackup(), config.BackupDataLeaseTime)
        dataSpace.write(dataEntry.createInfoBackup(), config.BackupDataLeaseTime)
      }
    }

    /**
      * If a worker is already defined, sends the input to be processed,
      * otherwise, a new thread is created and then the input is send.
      */
    def process(dataEntries: Vector[DataEntry], activity: ActivityWorker): Unit = {
      val workerThread: WorkerThread =
        processing match {
          case Some(workerThread: WorkerThread) =>
            workerThread
          case _ =>
            val workerThread = new WorkerThread(this, config)
            processing = Some(workerThread)
            workerThread.start()
            workerThread
        }
      workerThread.sendInput(activity, dataEntries)
      templateUpdateAct = templateUpdateAct.setPayload(
        NodeInfoType(nodeId, activity.id, Some(dataEntries.head.injectId, dataEntries.head.orderId)))

      var initProcessTime = System.currentTimeMillis()
      var initDataProcessTime = System.currentTimeMillis()

      while (workerThread.threadIsBusy) {
        handleSignals()
        val currentTime = System.currentTimeMillis()
        if (currentTime - initProcessTime > config.SendStillProcessingTime) {
          initProcessTime = currentTime
          updateNodeInfo(force = true)
        }
        if (currentTime - initDataProcessTime > config.RenewBackupEntriesTime) {
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
      templateUpdateAct = templateUpdateAct.setPayload(NodeInfoType(nodeId, activity.id, None))
    }

    /**
      * Checks in a period of time if this node needs to change
      */
    def checkNeedToChange(actId: String): Boolean = {
      val nowTime = System.currentTimeMillis()
      if (nowTime - checkTime > config.NodeCheckTableTime) {
        checkTime = nowTime
        signalSpace.read(templateTable, 0) match {
          case None => consensusAnalyser()
          case Some(ExoEntry(_, AnalyserTable(table, analyserConfig))) =>
            updateConfig(analyserConfig)
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
      * Returns a random activity id from the table
      */
    def getRandomActivity(table: TableType): Option[String] = {
      val tableList: List[(String, Int)] = table.toList
      if (tableList.isEmpty)
        None
      else {
        val total = math.max(1, tableList.unzip._2.sum)
        val n = 1.0 / tableList.size
        // excludes the current activity and others that probably don't need more nodes
        val list: List[(String, Int)] = tableList.filter(_._2.toDouble / total < n)

        if (list.isEmpty)
          Some(tableList(Random.nextInt(tableList.size))._1)
        else
          Some(list(Random.nextInt(list.size))._1)
      }
    }

    /**
      * Sets this node to process input of a specific activity.
      *
      * Also updates the templates and worker.
      */
    def setActivity(activityId: String): Unit = {
      signalSpace.read(templateAct.setMarker(activityId), 0) match {
        case None =>
          // there is a problem with the activity or the graph was already removed
          val graphId = activityId.substring(0, activityId.indexOf(":"))
          signalSpace.read(GraphEntry(graphId, null), 0) match {
            case None =>
            // The graph that contains this activity doesn't exist anymore, just ignore
            case Some(_) =>
              println(s"$nodeFullId;ActivitySignal for activity $activityId not found in SignalSpace")
              Log.writeLog(LogActivityNotFound(nodeId, activityId))
              Thread.sleep(config.ErrorSleepTime)
          }
        case Some(entry) => entry.payload match {
          case activitySignal: ActivitySignal =>
            ActivityCache.getActivity(activitySignal.name) match {
              case Some(activity) =>
                templateUpdateAct = templateUpdateAct.setPayload(NodeInfoType(nodeId, activityId, None))
                templateData = DataEntry(activityId, null, null, null, null)
                val activityWorker =
                  new ActivityWorker(activityId, activitySignal.actType, activity,
                    activitySignal.params, activitySignal.outMarkers)
                Log.writeLog(LogChangedAct(nodeId, fromId, activityId, "Node changed Activity"))
                activitySignal.inMarkers match {
                  case Vector(_) =>
                    worker = ConsecutiveWork(activityWorker)
                  case actsFrom: Vector[String] =>
                    worker = JoinWork(activityWorker, actsFrom)
                }
                // send the updated information
                updateNodeInfo(force = true)
                sleepTime = config.NodeMinSleepTime
              case None =>
                println(nodeFullId + ";Class could not be loaded: " + activitySignal.name)
                Log.writeLog(LogClassNotLoaded(nodeId, activitySignal.name))
                Thread.sleep(config.ErrorSleepTime)
            }
        }
      }
    }

    /**
      * Sets this node back to neutral mode.
      */
    def setNeutralMode(): Unit = {
      worker = NoWork
      templateUpdateAct = templateUpdateAct.setPayload(NodeInfoType(nodeId, UndefinedActId, None))
      updateNodeInfo(force = true)
    }
  }

}

object CliftonNode {

  var Debug: Boolean = false

  def debug(nodeId: String, msg: => String): Unit = {
    if (Debug) {
      println(new Date().toString, nodeId, msg)
    }
  }

}