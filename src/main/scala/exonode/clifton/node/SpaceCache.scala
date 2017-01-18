package exonode.clifton.node

import com.zink.fly.kit.{FlyFactory, FlyFinder}

import scala.collection.mutable

/**
  * Created by #ScalaTeam on 20/12/2016.
  *
  * Default parameters are set to LocalHost but can be changed
  *
  * SignalSpace => saves the graphRepresentation, Log infos, nodes info
  * DataSpace => saves the result of all activities
  * JarSpace => saves FlyClassEntry and FlyJarEntry of all jars
  *
  */
object SpaceCache {

  private val data = "DataSpace"
  private val jar = "JarSpace"
  private val signal = "SignalSpace"
  private val spaceMap = new mutable.HashMap[String, FlyOption]()

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
  private def getSpace(tag: String, host: String): FlyOption = {
    spaceMap.get(tag) match {
      case Some(space) => space
      case None => {
        try {
          if (host.isEmpty) {
            val finder: FlyFinder = new FlyFinder()
            spaceMap.put(tag, FlyScala(finder.find(tag)))
          } else
            spaceMap.put(tag, FlyScala(FlyFactory.makeFly(host)))
          spaceMap(tag)
        } catch {
          case e: Exception =>
            //Log.error("Failed to locate space")
            throw new Exception("Failed to locate space")
        }
      }
    }
  }

  def getSignalSpace: FlyOption = getSpace(signal, signalHost)

  def getDataSpace: FlyOption = getSpace(data, dataHost)

  def getJarSpace: FlyOption = getSpace(jar, jarHost)
}
