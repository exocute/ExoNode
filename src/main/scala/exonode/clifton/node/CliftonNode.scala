package exonode.clifton.node

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}
import java.util.{Date, UUID}

import exonode.clifton.config.ProtocolConfig
import exonode.clifton.config.ProtocolConfig._
import exonode.clifton.node.entries.{DataEntry, ExoEntry, GraphEntry}
import exonode.clifton.node.work.{BusyWorking, _}
import exonode.clifton.signals.Log.{Log, _}
import exonode.clifton.signals.{NodeSignal, _}
import twotails.mutualrec

import scala.annotation.tailrec
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

  val waitQueue: BlockingQueue[Unit] = new LinkedBlockingQueue[Unit]()
  private var config: ProtocolConfig = ProtocolConfig.Default

  private type ProcessingType = Option[Thread with BusyWorking]

  private case class NodeState(sleepTime: Long,
                               checkChangeTime: Long,
                               sendInfoTime: Long,
                               killNodeWhenIdle: Boolean,
                               workType: WorkType,
                               processing: ProcessingType,
                               templateData: DataEntry,
                               templateUpdateAct: ExoEntry[NodeInfoType]) {

    def setSleepTime(newSleepTime: Long): NodeState = copy(sleepTime = newSleepTime)

    def setCheckChangeTime(newCheckChangeTime: Long): NodeState = copy(checkChangeTime = newCheckChangeTime)

    def setSendInfoTime(newSendInfoTime: Long): NodeState = copy(sendInfoTime = newSendInfoTime)

    def setKillNodeWhenIdle(): NodeState = copy(killNodeWhenIdle = true)

    def setWorkType(newWorkType: WorkType): NodeState = copy(workType = newWorkType)

    def setProcessing(newProcessing: ProcessingType): NodeState = copy(processing = newProcessing)

    def setTemplateData(newTemplateData: DataEntry): NodeState = copy(templateData = newTemplateData)

    def setTemplateUpdateAct(newTemplateUpdateAct: ExoEntry[NodeInfoType]): NodeState =
      copy(templateUpdateAct = newTemplateUpdateAct)

  }

  private class KillSignalException(val nodeState: NodeState) extends Exception

  //  // TODO: remove this
  //  private var killWhenIdle = false

  //templates to search in spaces
  private val templateAct = ExoEntry[ActivitySignal]("", null)
  private val templateTable = ExoEntry[AnalyserTable](TableMarker, null)
  private val templateMySignals = ExoEntry[NodeSignal](nodeId, null)
  private val templateNodeSignals = ExoEntry[NodeSignal](NodeSignalMarker, null)
  private val templateWantAnalyser = ExoEntry[String](WantToBeAnalyserMarker, null)

  def fromId(nodeState: NodeState): String =
    if (nodeState.workType.hasWork)
      nodeState.workType.activity.id
    else
      nodeState.processing match {
        case Some(_: Analyser) => AnalyserMarker
        case _ => UndefinedActId
      }

  def nodeFullId(initialNodeState: NodeState): String = {
    s"$nodeId(${fromId(initialNodeState)})"
  }

  /**
    * Starting point of a CliftonNode
    */
  override def run(): Unit = {
    val templateData: DataEntry = DataEntry(null, null, null, null, null)
    val templateUpdateAct = ExoEntry[NodeInfoType](InfoMarker, NodeInfoType(nodeId, UndefinedActId, None))

    //current worker definitions
    val workType: WorkType = NoWork
    val processing: ProcessingType = None

    //times initializer
    val sleepTime = config.NodeMinSleepTime
    val checkChangeTime = System.currentTimeMillis()
    val sendInfoTime = 0L

    val nodeState =
      NodeState(sleepTime,
        checkChangeTime,
        sendInfoTime,
        killNodeWhenIdle = false,
        workType,
        processing,
        templateData,
        templateUpdateAct)

    val bootMessage = s"Node started"
    println(s"${nodeFullId(nodeState)};$bootMessage")
    Log.writeLog(LogStartedNode(nodeId))

    try {
      mainLoop(nodeState)
    } catch {
      case killSignalException: KillSignalException =>
        println(s"Going to stop: ${nodeFullId(killSignalException.nodeState)}")
    }
  }

  @mutualrec
  private def mainLoop(initialNodeState: NodeState): Unit = {
    val handledNodeState = handleSignals(initialNodeState)

    if (handledNodeState.killNodeWhenIdle)
      killOwnSignal(handledNodeState)

    val finalNodeState =
      handledNodeState.workType match {
        case NoWork =>
          // worker is not defined yet
          tryToBeAWorker(handledNodeState)
        case w if w.hasWork =>
          val newNodeState = searchForDataToProcess(handledNodeState)
          val (changedNodeState, success) = checkNeedToChange(newNodeState, newNodeState.workType.activity.id)
          if (!success) {
            sleepForAWhile(updateNodeInfo(changedNodeState))
          } else
            changedNodeState
      }

    mainLoop(finalNodeState)
  }

  @mutualrec
  private def searchForDataToProcess(initialNodeState: NodeState): NodeState = {
    val ((updatedNodeState, hasWork), activityId) =
      initialNodeState.workType match {
        case work @ ConsecutiveWork(activity) =>
          (consecutiveWork(initialNodeState, work), activity.id)
        case work @ JoinWork(activity, _) =>
          (tryToDoJoin(initialNodeState, work), activity.id)
        case _ => //ignore
          ((initialNodeState, false), "")
      }

    if (hasWork) {
      //checks if it needs to change mode
      val (newNodeState, _) = checkNeedToChange(updatedNodeState, activityId)
      searchForDataToProcess(newNodeState)
    } else
      initialNodeState
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
  private def updateNodeInfo(nodeState: NodeState, force: Boolean = false): NodeState = {
    val currentTime = System.currentTimeMillis()
    if (force || currentTime - nodeState.sendInfoTime > config.NodeCheckTableTime) {
      signalSpace.write(nodeState.templateUpdateAct, config.NodeInfoLeaseTime)
      nodeState.setSendInfoTime(currentTime)
    } else
      nodeState
  }

  private def sleepForAWhile(nodeState: NodeState): NodeState = {
    // if nothing was found, it will sleep for a while
    Thread.sleep(nodeState.sleepTime)
    nodeState.setSleepTime(math.min(nodeState.sleepTime * 2, config.NodeMaxSleepTime))
  }

  private def tryToBeAWorker(initialNodeState: NodeState): NodeState = {
    signalSpace.read(templateTable, 0) match {
      case Some(ExoEntry(_, AnalyserTable(table, analyserConfig))) =>
        updateConfig(analyserConfig)
        val filteredTable = filterActivities(table)
        if (filteredTable.isEmpty) {
          // There are no activities available
          sleepForAWhile(updateNodeInfo(initialNodeState))
        } else {
          // reset sleep time
          val nodeState = initialNodeState.setSleepTime(config.NodeMinSleepTime)

          getRandomActivity(filteredTable) match {
            case None =>
              // no valid activity found
              nodeState
            case Some(act) =>
              setActivity(nodeState, act)
          }
        }
      case None =>
        sleepForAWhile(consensusAnalyser(initialNodeState))
    }
  }

  /**
    * Starts the consensus algorithm to determine which node will be the next Analyser
    */
  private def consensusAnalyser(initialNodeState: NodeState): NodeState = {
    Thread.sleep(config.consensusRandomSleepTime())
    auxConsensusAnalyser()
    Thread.sleep(config.ConsensusMaxSleepTime)

    @tailrec
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
                  transformIntoAnalyser(initialNodeState)
                } else {
                  // The consensus have failed (want-to-be-analyser entry have timeout from the space?)
                  debug("found that consensus has failed")
                  signalSpace.take(TableMarker, 0)
                }
              } else {
                debug("found a table when it was trying to be an analyser")
              }
            }
          } else {
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

    initialNodeState
  }

  /**
    * Transforms this node into an [[exonode.clifton.node.work.AnalyserThread AnalyserThread]]
    */
  private def transformIntoAnalyser(initialNodeState: NodeState): Unit = {
    initialNodeState.processing.foreach { thread =>
      thread.interrupt()
      thread.join()
    }

    val analyserThread = new AnalyserThread(nodeId)
    val nodeState = initialNodeState.setProcessing(Some(analyserThread))
    analyserThread.start()

    val analyserBootMessage = "Node changed to analyser mode"
    println(s"${nodeFullId(nodeState)};$analyserBootMessage")
    Log.writeLog(LogChangedAct(nodeId, fromId(nodeState), AnalyserMarker, analyserBootMessage))

    @tailrec
    def infiniteLoop(nodeState: NodeState) {
      val newState = handleSignals(nodeState)
      Thread.sleep(config.AnalyserSleepTime)
      infiniteLoop(newState)
    }

    infiniteLoop(nodeState)
  }

  private def consecutiveWork(initialNodeState: NodeState, consWork: ConsecutiveWork): (NodeState, Boolean) = {
    //get something to process
    takeAndBackup(initialNodeState.templateData) match {
      case Some(dataEntry) =>
        //if something was found
        val nodeState = initialNodeState.setSleepTime(config.NodeMinSleepTime)
        (process(nodeState, Vector(dataEntry), consWork.activity), true)
      case None =>
        (initialNodeState, false)
    }
  }

  /**
    * Tries to take a [[exonode.clifton.node.entries.DataEntry DataEntry]] from the space and
    * writes a backup of it.
    */
  private def takeAndBackup(tempData: DataEntry): Option[DataEntry] = {
    dataSpace.take(tempData, 0) match {
      case Some(dataEntry) =>
        dataSpace.write(dataEntry.createBackup(), config.BackupDataLeaseTime)
        dataSpace.write(dataEntry.createInfoBackup(), config.BackupDataLeaseTime)
        Some(dataEntry)
      case None => None
    }
  }

  /**
    * Reads signals from the space that are generic to every node or specific to this node.
    */
  private def handleSignals(initialNodeState: NodeState): NodeState = {
    val updatedNodeState =
      signalSpace.take(templateMySignals, 0) match {
        case None => initialNodeState
        case Some(ExoEntry(_, mySignal)) => processSignal(initialNodeState, mySignal)
      }

    if (updatedNodeState.killNodeWhenIdle) {
      signalSpace.take(templateNodeSignals, 0) match {
        case None => updatedNodeState
        case Some(ExoEntry(_, nodeSignal)) => processSignal(initialNodeState, nodeSignal)
      }
    } else
      updatedNodeState
  }

  /**
    * Kills this node.
    */
  private def killOwnSignal(initialNodeState: NodeState): Nothing = {
    val shutdownMsg = "Node is going to shutdown"
    println(s"${nodeFullId(initialNodeState)};$shutdownMsg")
    Log.writeLog(LogNodeShutdown(nodeId, shutdownMsg))
    initialNodeState.processing match {
      case None =>
      case Some(analyserThread: Analyser) =>
        analyserThread.cancelThread()
        analyserThread.join()
      case Some(thread) =>
        thread.interrupt()
        thread.join()
    }
    throw new KillSignalException(initialNodeState)
  }

  /**
    * If it's a KillSignal this node immediately aborts and dies.
    * If it's a KillGraceFullSignal then killWhenIdle is set to true,
    * so that before receiving something new to process this node will die.
    */
  private def processSignal(initialNodeState: NodeState, nodeSignal: NodeSignal): NodeState = {
    nodeSignal match {
      case KillSignal => killOwnSignal(initialNodeState)
      case KillGracefullSignal =>
        initialNodeState.setKillNodeWhenIdle()
    }
  }

  private def tryToDoJoin(initialNodeState: NodeState, joinWork: JoinWork): (NodeState, Boolean) = {
    val (updatedNodeState, success) = tryToReadAll(initialNodeState, Random.shuffle(joinWork.actsFrom))
    if (success) {
      tryToTakeAll(updatedNodeState, joinWork.actsFrom) match {
        case Vector() =>
          // Other node was faster to take the data
          // Just restart the process
          (updatedNodeState, true)
        case values: Vector[DataEntry] =>
          if (values.size == joinWork.actsFrom.size) {
            // we have all values, so we can continue
            val finalNodeState = updatedNodeState.setSleepTime(config.NodeMinSleepTime)
            (process(finalNodeState, values, joinWork.activity), true)
          } else {
            // some values were lost ?
            val msg = s"Data was missing with injectId=${values.head.injectId}, " +
              s"${values.size} values found, ${joinWork.actsFrom.size} values expected"
            println(nodeFullId(updatedNodeState) + ";" + msg)
            Log.writeLog(LogValuesLost(nodeId, msg))
            (updatedNodeState, true)
          }
      }
    } else
      (updatedNodeState, false)
  }

  /**
    * Tries to read all the results of an injectId that should do a join operation
    *
    * @return true if all activities of the join are already available in the space
    */
  private def tryToReadAll(initialNodeState: NodeState, actsFrom: Vector[String]): (NodeState, Boolean) = {
    dataSpace.read(initialNodeState.templateData.setFrom(actsFrom(0)).setInjectId(null), 0) match {
      case Some(dataEntry) =>
        val updatedNodeState = initialNodeState.setTemplateData(
          initialNodeState.templateData.setInjectId(dataEntry.injectId))

        val foundAll =
          (1 until actsFrom.size).forall {
            actIndex => dataSpace.read(updatedNodeState.templateData.setFrom(actsFrom(actIndex)), 0).isDefined
          }

        (updatedNodeState, foundAll)
      case None =>
        (initialNodeState, false)
    }
  }

  /**
    * Tries to take all the results of an injectId that should do a join operation
    *
    * @return if all activities of the join were successfully taken from the space it returns
    *         a vector with them
    */
  private def tryToTakeAll(initialNodeState: NodeState, actsFrom: Vector[String]): Vector[DataEntry] = {
    @tailrec
    def tryToTakeAllAux(index: Int, acc: Vector[DataEntry]): Vector[DataEntry] = {
      if (index >= actsFrom.size) {
        acc
      } else {
        val from = actsFrom(index)
        takeAndBackup(initialNodeState.templateData.setFrom(from)) match {
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
  private def renewBackup(dataEntries: Vector[DataEntry]): Unit = {
    for (dataEntry <- dataEntries) {
      dataSpace.write(dataEntry.createBackup(), config.BackupDataLeaseTime)
      dataSpace.write(dataEntry.createInfoBackup(), config.BackupDataLeaseTime)
    }
  }

  /**
    * If a worker is already defined, sends the input to be processed,
    * otherwise, a new thread is created and then the input is sent.
    */
  private def process(initialNodeState: NodeState,
                      dataEntries: Vector[DataEntry],
                      activity: ActivityWorker): NodeState = {

    val workerThread: WorkerThread =
      initialNodeState.processing match {
        case Some(workerThread: WorkerThread) =>
          workerThread
        case _ =>
          val workerThread = new WorkerThread(this, config)
          workerThread.start()
          workerThread
      }

    workerThread.sendInput(activity, dataEntries)

    val workerNodeState =
      initialNodeState
        .setProcessing(Some(workerThread))
        .setTemplateUpdateAct(initialNodeState.templateUpdateAct.setPayload(
          NodeInfoType(nodeId, activity.id, Some(dataEntries.head.injectId, dataEntries.head.orderId))))

    @tailrec
    def workerLoop(initialNodeState: NodeState, initProcessTime: Long, initDataProcessTime: Long): NodeState = {
      if (workerThread.threadIsBusy) {
        val nodeState = handleSignals(initialNodeState)
        val currentTime = System.currentTimeMillis()

        val (updatedNodeState, updatedProcessTime) =
          if (currentTime - initProcessTime > config.SendStillProcessingTime) {
            (updateNodeInfo(nodeState, force = true), currentTime)
          } else
            (nodeState, initProcessTime)

        val updatedDataProcessTime =
          if (currentTime - initDataProcessTime > config.RenewBackupEntriesTime) {
            renewBackup(dataEntries)
            currentTime
          } else
            initDataProcessTime
        try {
          waitQueue.poll(10000, TimeUnit.MILLISECONDS)
        } catch {
          case _: InterruptedException =>
          // thread finished processing
          // So, back to normal mode
        }
        workerLoop(updatedNodeState, updatedProcessTime, updatedDataProcessTime)
      } else {
        initialNodeState
      }
    }

    workerLoop(workerNodeState, System.currentTimeMillis(), System.currentTimeMillis())
      .setTemplateUpdateAct(workerNodeState.templateUpdateAct.setPayload(NodeInfoType(nodeId, activity.id, None)))
  }

  /**
    * Checks in a period of time if this node needs to be changed
    */
  private def checkNeedToChange(initialNodeState: NodeState, actId: String): (NodeState, Boolean) = {
    val currentTime = System.currentTimeMillis()
    if (currentTime - initialNodeState.checkChangeTime > config.NodeCheckTableTime) {
      val changedNodeState = initialNodeState.setCheckChangeTime(currentTime)

      val finalNodeState: NodeState =
        signalSpace.read(templateTable, 0) match {
          case None => consensusAnalyser(changedNodeState)
          case Some(ExoEntry(_, AnalyserTable(table, analyserConfig))) =>
            updateConfig(analyserConfig)
            val filteredTable = filterActivities(table)

            //Test if the table still contains the current activity
            if (!filteredTable.contains(actId)) {
              getRandomActivity(filteredTable) match {
                case None => setNeutralMode(changedNodeState)
                case Some(act) => setActivity(changedNodeState, act)
              }
            } else {
              // there needs to be at least one activity to jump to
              if (filteredTable.size <= 1) {
                changedNodeState
              } else {
                val totalNodes = filteredTable.values.sum
                val n = 1.0 / filteredTable.size
                val q = filteredTable(actId).toDouble / totalNodes
                val uw = 1.0 / totalNodes
                //checks if its need to update function
                if (q > n && Random.nextDouble() < (q - n) / q) {
                  //should i transform
                  getRandomActivity(filteredTable) match {
                    case None =>
                      setNeutralMode(changedNodeState)
                    case Some(newAct) =>
                      val qNew = filteredTable(newAct).toDouble / totalNodes
                      if (q - uw >= n || qNew + uw <= n) {
                        setActivity(changedNodeState, newAct)
                      } else
                        changedNodeState
                  }
                } else
                  changedNodeState
              }
            }
        }

      (finalNodeState, true)
    } else
      (initialNodeState, false)
  }

  /**
    * Returns a random activity id from the table
    */
  private def getRandomActivity(table: TableType): Option[String] = {
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
  private def setActivity(initialNodeState: NodeState, activityId: String): NodeState = {
    signalSpace.read(templateAct.setMarker(activityId), 0) match {
      case None =>
        // there is a problem with the activity or the graph was already removed
        val graphId = activityId.substring(0, activityId.indexOf(":"))
        signalSpace.read(GraphEntry(graphId, null), 0) match {
          case None =>
          // The graph that contains this activity doesn't exist anymore, just ignore
          case Some(_) =>
            println(s"${nodeFullId(initialNodeState)};ActivitySignal for activity $activityId not found in SignalSpace")
            Log.writeLog(LogActivityNotFound(nodeId, activityId))
            Thread.sleep(config.ErrorSleepTime)
        }
        initialNodeState
      case Some(entry) => entry.payload match {
        case activitySignal: ActivitySignal =>
          ActivityCache.getActivity(activitySignal.name) match {
            case Some(activity) =>
              val updatedNodeState =
                initialNodeState
                  .setTemplateUpdateAct(initialNodeState.templateUpdateAct.setPayload(NodeInfoType(nodeId, activityId, None)))
                  .setTemplateData(DataEntry(activityId, null, null, null, null))
              Log.writeLog(LogChangedAct(nodeId, fromId(updatedNodeState), activityId, "Node changed Activity"))

              val activityWorker =
                new ActivityWorker(activityId, activitySignal.actType, activity,
                  activitySignal.params, activitySignal.outMarkers)

              val workerNodeState =
                updatedNodeState.setWorkType(
                  activitySignal.inMarkers match {
                    case Vector(_) =>
                      ConsecutiveWork(activityWorker)
                    case actsFrom: Vector[String] =>
                      JoinWork(activityWorker, actsFrom)
                  })

              // send the updated information
              updateNodeInfo(workerNodeState, force = true).setSleepTime(config.NodeMinSleepTime)
            case None =>
              println(nodeFullId(initialNodeState) + ";Class could not be loaded: " + activitySignal.name)
              Log.writeLog(LogClassNotLoaded(nodeId, activitySignal.name))
              Thread.sleep(config.ErrorSleepTime)
              initialNodeState
          }
      }
    }
  }

  /**
    * Sets this node back to neutral mode.
    */
  private def setNeutralMode(initialNodeState: NodeState): NodeState = {
    val updatedNodeState =
      initialNodeState
        .setWorkType(NoWork)
        .setTemplateUpdateAct(initialNodeState.templateUpdateAct.setPayload(NodeInfoType(nodeId, UndefinedActId, None)))
    updateNodeInfo(updatedNodeState, force = true)
  }

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

}

object CliftonNode {

  var Debug: Boolean = false

  def debug(nodeId: String, msg: => String): Unit = {
    if (Debug) {
      println(new Date().toString, nodeId, msg)
    }
  }

}