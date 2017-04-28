package exonode.clifton.node

import exocute.Activity

import scala.collection.mutable

/**
  * Created by #GrowinScala
  *
  * Takes a activity name and finds it in the space
  */
object ActivityCache {

  private val MAX_SIZE = 200
  private val cache = mutable.HashMap[String, Activity]()
  private var cacheOrder = Vector[String]()

  def getActivity(name: String): Option[Activity] = {
    cache.get(name) match {
      case None =>
        try {
          // go get the required jar
          val cl = new CliftonClassLoader()
          val jar = CliftonClassLoader.getJarFromSpace(name)
          if (jar != null) {
            val acl = cl.loadClass(name)
            acl.newInstance match {
              case activity: Activity =>
                val id = activity.getClass.getName
                cacheOrder = cacheOrder :+ id
                if (cache.size == MAX_SIZE) {
                  cache.remove(cacheOrder.head)
                  cacheOrder = cacheOrder.tail
                }
                cache.put(id, activity)
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
