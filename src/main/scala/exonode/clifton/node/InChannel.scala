package exonode.clifton.node

import java.io.Serializable

import com.zink.fly.Fly

/**
  * Created by #ScalaTeam on 20/12/2016.
  */
abstract class InChannel(marker: String) {

  private val space: Fly = getSpace
  private val template: ExoEntry = new ExoEntry(marker, null)

  def getSpace: Fly

  private var TAKE_TIME = 0

  def getObject(): Serializable = {
    if (space != null) {
      var result = new ExoEntry()
      try {
        result = space.take(template, TAKE_TIME)
      } catch {
        case e: Exception => e.printStackTrace()
      }

      if (result != null) {
        TAKE_TIME = 0
        try {
          return result.payload
        } catch {
          case e: Exception => e.printStackTrace()
        }
      } else TAKE_TIME = 1000
    }
    Log.error("SignalSpace not defined")
    null
  }

}


