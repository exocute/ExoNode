package exonode.clifton.node

import java.io.Serializable

/**
  * Created by #ScalaTeam on 20/12/2016.
  */
class ExoEntry(var marker: String, var payload: Serializable) {
  def this() = {
    this(null, null)
  }

  override def toString: String = marker + ":" + payload
}
