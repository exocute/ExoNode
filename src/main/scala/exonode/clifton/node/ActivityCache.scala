package exonode.clifton.node

import exocute.Activity

import scala.collection.mutable

/**
  * Created by #GrowinScala
  *
  * Takes a activity name and finds it in the space
  */
object ActivityCache {

  private val _cache = new mutable.HashMap[String, Activity]()

  def getActivity(name: String): Option[Activity] = {
    _cache.get(name) match {
      case None =>
        try {
          // go get the required jar
          val cl = new CliftonClassLoader()
          val jar = CliftonClassLoader.getJarFromSpace(name)
          if (jar != null) {
            val acl = cl.loadClass(name)
            acl.newInstance match {
              case activity: Activity =>
                _cache.put(activity.getClass.getName, activity)
                Some(activity)
              case _ => None
            }
          } else
            None
        } catch {
          case e: Exception =>
            e.printStackTrace()
            None
        }
      case some => some
    }

  }

  def getActivityFromLocalFile(fileBytes: Array[Byte], activityName: String): Option[Activity] = {
    try {
      val cl = new CliftonClassLoader()
      cl.init(fileBytes)
      val activityClass = cl.findClass(activityName)
      activityClass.newInstance match {
        case activity: Activity => Some(activity)
        case None => None
      }
    } catch {
      case _: Exception => None
    }
  }
}
