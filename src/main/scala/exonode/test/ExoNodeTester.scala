package exonode.test

import exonode.clifton.Protocol._
import exonode.clifton.node.{CliftonNode, SpaceCache}
import org.scalatest.FlatSpec
import java.io.Serializable
import java.util.Random

import exonode.clifton.node.entries.ExoEntry
import exonode.clifton.signals.KillSignal

import scala.collection.immutable.HashMap

/**
  * Created by #ScalaTeam on 17/01/2017.
  */
class ExoNodeTester extends FlatSpec {

  val space = SpaceCache.getSignalSpace

  val TEST_TIME = 60 * 1000

  val tab = List[(String,Int)]()

  val actDistributionTable: TableType = {
    for {
      (entryNo, _) <- tab
      if entryNo != ANALYSER_ACT_ID
    } yield entryNo -> 0
  } + (ANALYSER_ACT_ID -> 0)

  implicit def seqToHashMap[A, B](seq: Seq[(A, B)]): HashMap[A, B] = {
    HashMap(seq: _*)
  }

  def writeToSpace(marker: String, input: Serializable) = {
    space.write(ExoEntry(marker, input), TEST_TIME)
  }

  val tabEntry = ExoEntry(TABLE_MARKER, null)

  def readTableFromSpace(): ExoEntry = {
    space.read(tabEntry, 0L).get
  }

  val genericEntry = new ExoEntry(null, null)

  def cleanSpace(): Unit = {
    while (space.take(genericEntry, 0L).isDefined) ()
  }

  def launchNNodes(nodes: Int) : List[CliftonNode] = {
    val nodesList = for{
      x <- 1 to nodes
    } yield new CliftonNode()
    nodesList.foreach(cn => cn.start())
    nodesList.toList
  }

  def killNNodes(nodes : List[CliftonNode]) = {
    nodes.foreach(node => writeToSpace(node.nodeId,KillSignal))
    nodes.foreach(node => node.join())
  }

  "Launch5Nodes" should "Launch 5 nodes" in {
    cleanSpace()
    writeToSpace(TABLE_MARKER, actDistributionTable)
    val ids = launchNNodes(5)
    Thread.sleep(5 * 1000)
    val newTable = readTableFromSpace().payload.asInstanceOf[TableType]
    println("killing nodes")
    killNNodes(ids)
    assert(newTable.foldLeft(0)(_ + _._2) == 5)
  }

  "analyserStarted" should "startAnalyser" in {
    cleanSpace()
    writeToSpace(TABLE_MARKER, actDistributionTable)
    val id = launchNNodes(1)
    Thread.sleep(5 * 1000)
    val newTable = readTableFromSpace().payload.asInstanceOf[TableType]
    killNNodes(id)
    assert(newTable(ANALYSER_ACT_ID) == 1)
  }


  "killAnalyser" should "killAnalyser" in {
    cleanSpace()
    writeToSpace(TABLE_MARKER, actDistributionTable)
    val analyser = new CliftonNode()
    analyser.start()
    Thread.sleep(5 * 1000)
    val ids = launchNNodes(5)

    //kill analyser
    writeToSpace(analyser.nodeId, KillSignal)
    Thread.sleep(TABLE_LEASE_TIME)
    writeToSpace(TABLE_MARKER, actDistributionTable)
    Thread.sleep(5 * 1000)
    val newTable = readTableFromSpace().payload.asInstanceOf[TableType]
    killNNodes(ids)
    assert(newTable(ANALYSER_ACT_ID) == 1)
  }

}
