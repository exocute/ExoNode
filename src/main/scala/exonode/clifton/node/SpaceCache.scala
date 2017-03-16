package exonode.clifton.node

import com.zink.fly.kit.{FlyFactory, FlyFinder}
import com.zink.scala.fly.ScalaFly
import exonode.clifton.node.entries.{BackupEntry, BackupInfoEntry, DataEntry, ExoEntry}
import exonode.distributer.{FlyClassEntry, FlyJarEntry}

import scala.collection.mutable

/**
  * Created by #GrowinScala
  *
  * Default parameters are set to LocalHost but can be changed
  *
  * SignalSpace => saves the graph representation, Log infos, nodes info
  * DataSpace => saves the data and backups of all activities
  * JarSpace => saves FlyClassEntry and FlyJarEntry of all jars
  *
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
    * finds a flyspace by the host defined
    *
    * @param tag
    * @param host
    * @return if its found returns the flyspace on Host
    */
  private def getSpace(tag: String, host: String): ScalaFly = {
    spaceMap.get(tag) match {
      case Some(space) => space
      case None =>
        try {
          if (host.isEmpty) {
            val finder: FlyFinder = new FlyFinder()
            spaceMap.put(tag, ScalaFly(finder.find(tag)))
          } else
            spaceMap.put(tag, ScalaFly(FlyFactory.makeFly(host)))
          spaceMap(tag)
        } catch {
          case e: Exception =>
            //Log.error("Failed to locate space")
            throw new Exception("Failed to locate space")
        }
    }
  }

  def getSignalSpace: ScalaFly = getSpace(signal, signalHost)

  def getDataSpace: ScalaFly = getSpace(data, dataHost)

  def getJarSpace: ScalaFly = getSpace(jar, jarHost)

  def cleanAllSpaces(): Unit = {
    def clean(space: ScalaFly, cleanTemplate: AnyRef): Unit = {
      while (space.take(cleanTemplate, 0).isDefined) {}
    }

    clean(getJarSpace, FlyJarEntry(null, null))
    clean(getJarSpace, FlyClassEntry(null, null))
    clean(getDataSpace, DataEntry(null, null, null, null, null))
    clean(getDataSpace, BackupEntry(null, null, null, null, null))
    clean(getDataSpace, BackupInfoEntry(null, null, null, null))
    clean(getSignalSpace, ExoEntry(null, null))
  }
}
