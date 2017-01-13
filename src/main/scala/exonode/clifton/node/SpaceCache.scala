package exonode.clifton.node

import com.zink.fly.Fly
import com.zink.fly.kit.{FlyFactory, FlyFinder}

import scala.collection.mutable

/**
  * Created by #ScalaTeam on 20/12/2016.
  */
object SpaceCache {

  private val data = "DataSpace"
  private val jar = "JarSpace"
  private val signal = "SignalSpace"
  private val spaceMap = new mutable.HashMap[String, Fly]()

  var signalHost: String = "localhost"
  var jarHost: String = "localhost"
  var dataHost: String = "localhost"

  private def getSpace(tag: String, host: String): Fly = {
    spaceMap.get(tag) match {
      case Some(space) => space
      case None => {
        try {
          if (host.isEmpty) {
            val finder: FlyFinder = new FlyFinder()
            spaceMap.put(tag, finder.find(tag))
          } else
            spaceMap.put(tag, FlyFactory.makeFly(host))
          spaceMap(tag)
        } catch {
          case e: Exception =>
            //Log.error("Failed to locate space")
            throw new Exception("Failed to locate space")
        }
      }
    }
  }

  def getSignalSpace: Fly = getSpace(signal, signalHost)

  def getDataSpace: Fly = getSpace(data, dataHost)

  def getJarSpace: Fly = getSpace(jar, jarHost)
}
