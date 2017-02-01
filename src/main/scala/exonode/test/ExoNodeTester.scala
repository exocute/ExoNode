package exonode.test

import java.io.Serializable

import exonode.clifton.Protocol._
import exonode.clifton.node.entries.ExoEntry
import exonode.clifton.node.{CliftonNode, SpaceCache}
import exonode.clifton.signals.KillSignal
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.collection.immutable.HashMap
import scala.language.implicitConversions

/**
  * Created by #ScalaTeam on 17/01/2017.
  */
class ExoNodeTester extends FlatSpec with BeforeAndAfter {

  private val space = SpaceCache.getSignalSpace

  private val TEST_TIME = 60 * 1000

  implicit def seqToHashMap[A, B](seq: Seq[(A, B)]): HashMap[A, B] = {
    HashMap(seq: _*)
  }

  private def writeToSpace(marker: String, input: Serializable) = {
    space.write(ExoEntry(marker, input), TEST_TIME)
  }

  private val tabEntry = ExoEntry(TABLE_MARKER, null)

  private def readTableFromSpace(): Option[ExoEntry] = {
    space.read(tabEntry, 0L)
  }

  private var allNodes = List[CliftonNode]()

  private def launchNNodes(nodes: Int): List[CliftonNode] = {
    val nodesList = for {
      _ <- 1 to nodes
    } yield {
      val node = new CliftonNode()
      allNodes = node :: allNodes
      node
    }
    nodesList.foreach(node => node.start())
    nodesList.toList
  }

  private def killNNodes(nodes: List[CliftonNode]): Unit = {
    nodes.foreach(node => writeToSpace(node.nodeId, KillSignal))
    nodes.foreach(node => node.join())
  }

  private val EXPECTED_TIME_TO_CONSENSUS = 10 * 1000 + CONSENSUS_MAX_SLEEP_TIME * (1 + CONSENSUS_LOOPS_TO_FINISH)

  //  def doTestAndCleanSpaces(title: String, description: String, test: => (List[CliftonNode], Boolean)): Unit = {
  //    title should description in {
  //      SpaceCache.cleanAllSpaces()
  //      val (ids, res) = test
  //      killNNodes(ids)
  //      assert(res)
  //    }
  //  }

  before {
    SpaceCache.cleanAllSpaces()
    println("Space Cleaned")
    Thread.sleep(500)
  }

  after {
    allNodes.foreach(node => writeToSpace(node.nodeId, KillSignal))
    allNodes.foreach(node => node.join())
    allNodes = Nil
  }

  "Launch 1 node" should "be the analyser" in {
    launchNNodes(1)
    Thread.sleep(EXPECTED_TIME_TO_CONSENSUS)
    assert(readTableFromSpace().isDefined)
  }

  "Launch 5 nodes" should "launch 5 nodes (4 + 1 analyser)" in {
    val N = 5
    val ids = launchNNodes(N)
    Thread.sleep(EXPECTED_TIME_TO_CONSENSUS)
    // time to read infos into table
    Thread.sleep(5 * 1000 + 3 * ANALYSER_SLEEP_TIME)
    readTableFromSpace() match {
      case None =>
        fail()
      case Some(entry) =>
        val newTable = entry.payload.asInstanceOf[TableType]
        assert(newTable.foldLeft(0)(_ + _._2) == N - 1)
    }
  }

  "Kill analyser" should "kill the analyser and other node should replace it" in {
    val analyserNode = launchNNodes(1).head
    Thread.sleep(EXPECTED_TIME_TO_CONSENSUS)

    launchNNodes(4)

    //kill analyser
    writeToSpace(analyserNode.nodeId, KillSignal)
    Thread.sleep(5 * 1000 + TABLE_LEASE_TIME)
    Thread.sleep(EXPECTED_TIME_TO_CONSENSUS)
    assert(readTableFromSpace().isDefined)
  }

}
