package exonode.clifton.node

import java.io.Serializable

/**
  * Created by #ScalaTeam on 20/12/2016.
  *
  * Generic Entry
  */
case class ExoEntry(marker: String, payload: Serializable) {

  def setMarker(newMarker: String) = ExoEntry(newMarker, payload)

  def setPayload(newPayload: Serializable) = ExoEntry(marker, newPayload)

  override def toString: String = marker + ":" + payload
}
