package exonode

import java.io.Serializable

import exonode.clifton.config.ProtocolConfig
import exonode.clifton.config.ProtocolConfig.AnalyserTable
import exonode.clifton.node.entries.ExoEntry
import exonode.clifton.node.{CliftonNode, SpaceCache}
import exonode.clifton.signals.KillSignal
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.language.implicitConversions

/**
  * Created by #GrowinScala
  */
class ExoNodeSpec extends FlatSpec with BeforeAndAfter {

  private val space = SpaceCache.getSignalSpace

  private val MAX_TIME_FOR_EACH_TEST = 60 * 60 * 1000

  private def writeToSpace(marker: String, input: Serializable) = {
    space.write(ExoEntry(marker, input), MAX_TIME_FOR_EACH_TEST)
  }

  private val tabEntry = ExoEntry[AnalyserTable](ProtocolConfig.TableMarker, null)
  private val config = ProtocolConfig.Default

  private def readTableFromSpace(): Option[ExoEntry[AnalyserTable]] = {
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

  private val EXPECTED_TIME_TO_CONSENSUS = 10 * 1000 +
    config.ConsensusMaxSleepTime * (1 + config.ConsensusLoopsToFinish)

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
    launchNNodes(N)
    Thread.sleep(EXPECTED_TIME_TO_CONSENSUS)
    // time to read infos into table
    Thread.sleep(5 * 1000 + 3 * config.AnalyserSleepTime)
    readTableFromSpace() match {
      case None =>
        fail()
      case Some(ExoEntry(_, AnalyserTable(newTable, _))) =>
        assert(newTable.foldLeft(0)(_ + _._2) == N - 1)
    }
  }

  "Kill analyser" should "kill the analyser and other node should replace it" in {
    val analyserNode = launchNNodes(1).head
    Thread.sleep(EXPECTED_TIME_TO_CONSENSUS)

    launchNNodes(4)

    //kill analyser
    killNodes(List(analyserNode))
    Thread.sleep(5 * 1000)
    Thread.sleep(EXPECTED_TIME_TO_CONSENSUS)
    assert(readTableFromSpace().isDefined)
  }

  {
    CliftonNode.Debug = true
    for (n <- 21 to 1 by -2) {
      s"Only 1 analyser with $n nodes" should "check if there is only one analyser in the space" in {
        only1Analyser(n)
      }
    }
    CliftonNode.Debug = false
  }

  private def only1Analyser(nodes: Int): Unit = {
    launchNNodes(nodes)
    Thread.sleep(EXPECTED_TIME_TO_CONSENSUS)

    Thread.sleep(60 * 1000)
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
