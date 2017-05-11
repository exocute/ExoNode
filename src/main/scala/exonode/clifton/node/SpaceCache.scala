package exonode.clifton.node

import com.zink.fly.kit.{FlyFactory, FlyFinder}
import com.zink.scala.fly.ScalaFly
import exonode.clifton.node.entries._
import exonode.distributer.{FlyClassEntry, FlyJarEntry}

import scala.collection.mutable

/**
  * Created by #GrowinScala
  *
  * Default parameters of all IPs are set to localhost but can be changed.
  *
  * SignalSpace => saves the graph representation, Log infos, nodes info
  * DataSpace => saves the data and backups of all activities
  * JarSpace => saves FlyClassEntry and FlyJarEntry of all jars
  */
object SpaceCache {

  private val data = "DataSpace"
  private val jar = "JarSpace"
  private val signal = "SignalSpace"
  private val spaceMap = new mutable.HashMap[String, ScalaFly]()

  var signalHost: String = "localhost"
  var jarHost: String = "localhost"
  var dataHost: String = "localhost"

  /**
    * Finds a flyspace by a tag.
    *
    * Provide a valid host when creating a new connection to a FlySpace.
    */
  private def getSpace(tag: String, host: String): ScalaFly = {
    spaceMap.getOrElseUpdate(tag,
      try {
        if (host.isEmpty) {
          val finder: FlyFinder = new FlyFinder()
          ScalaFly(finder.find(tag))
        } else
          ScalaFly(FlyFactory.makeFly(host))
      } catch {
        case e: Exception =>
          //Log.error("Failed to locate space")
          throw new Exception("Failed to locate space")
      })
  }

  def getSignalSpace: ScalaFly = getSpace(signal, signalHost)

  def getDataSpace: ScalaFly = getSpace(data, dataHost)

  def getJarSpace: ScalaFly = getSpace(jar, jarHost)

  /**
    * Cleans entries used by Exocute from all FlySpaces.
    */
  def cleanAllSpaces(): Unit = {
    def clean(space: ScalaFly, cleanTemplate: AnyRef): Unit = {
      while (space.take(cleanTemplate, 0).isDefined) {}
    }

    clean(getJarSpace, FlyJarEntry(null, null))
    clean(getJarSpace, FlyClassEntry(null, null))
    clean(getDataSpace, DataEntry(null, null, null, null, null))
    clean(getDataSpace, BackupEntry(null, null, null, null, null))
    clean(getDataSpace, BackupInfoEntry(null, null, null, null))
    clean(getDataSpace, FlatMapEntry(null, null, null))
    clean(getSignalSpace, ExoEntry(null, null))
    clean(getSignalSpace, GraphEntry(null, null))
  }
}
