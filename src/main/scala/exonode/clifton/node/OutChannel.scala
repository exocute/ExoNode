package exonode.clifton.node

import com.zink.fly.Fly

/**
  * Created by #ScalaTeam on 20/12/2016.
  */
abstract class OutChannel(marker: String, entryLifeTime: Long = 60 * 1000) {

  private val space: Fly = getSpace
  private val out = new ExoEntry(marker, null)

  def getSpace: Fly

  def putObject(obj: Serializable): Unit = {
    if (space != null && marker != null && obj != null) {
      out.payload = obj
      try {
        space.write(out, entryLifeTime)
      } catch {
        case e: Exception => Log.error("Output Channel Broken")
      }
    }
  }
}
