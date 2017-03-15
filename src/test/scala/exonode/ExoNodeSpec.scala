package exonode

import java.io.Serializable

import exonode.clifton.config.Protocol._
import exonode.clifton.node.entries.ExoEntry
import exonode.clifton.node.{CliftonNode, SpaceCache}
import exonode.clifton.signals.KillSignal
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.collection.immutable.HashMap
import scala.language.implicitConversions

/**
  * Created by #ScalaTeam on 17/01/2017.
  */
class ExoNodeSpec extends FlatSpec with BeforeAndAfter {

  private val space = SpaceCache.getSignalSpace

  private val MAX_TIME_FOR_EACH_TEST = 60 * 60 * 1000

  private implicit def seqToHashMap[A, B](seq: Seq[(A, B)]): HashMap[A, B] = {
    HashMap(seq: _*)
  }

  private def writeToSpace(marker: String, input: Serializable) = {
    space.write(ExoEntry(marker, input), MAX_TIME_FOR_EACH_TEST)
  }

  private val tabEntry = ExoEntry(TABLE_MARKER, null)

  private def readTableFromSpace(): Option[ExoEntry[_]] = {
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

  private def killNodes(nodes: List[CliftonNode]): Unit = {
    nodes.foreach(node => writeToSpace(node.nodeId, KillSignal))
    nodes.foreach(node => node.join())
  }

  private def killNodesById(nodeIds: List[String]): Unit = {
    nodeIds.foreach(nodeId => writeToSpace(nodeId, KillSignal))
  }

  private val EXPECTED_TIME_TO_CONSENSUS = 10 * 1000 + CONSENSUS_MAX_SLEEP_TIME * (1 + CONSENSUS_LOOPS_TO_FINISH)

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
    killNodesById(List(analyserNode.nodeId))
    Thread.sleep(5 * 1000)
    Thread.sleep(EXPECTED_TIME_TO_CONSENSUS)
    assert(readTableFromSpace().isDefined)
  }

  {
    CliftonNode.DEBUG = true
    for (n <- 20 to 1 by -1) {
      s"Only 1 analyser with $n nodes" should "check if there is only one analyser in the space" in {
        only1Analyser(n)
      }
    }
    CliftonNode.DEBUG = false
  }

  private def only1Analyser(nodes: Int): Unit = {
    launchNNodes(nodes)
    Thread.sleep(EXPECTED_TIME_TO_CONSENSUS)

    //kill analyser
    Thread.sleep(40 * 1000)
    assert {
      space.readMany(tabEntry, 2).size == 1 && {
        Thread.sleep(10 * 1000)
        space.readMany(tabEntry, 2).size == 1
      } && {
        Thread.sleep(10 * 1000)
        space.readMany(tabEntry, 2).size == 1
      }
    }
  }

}
