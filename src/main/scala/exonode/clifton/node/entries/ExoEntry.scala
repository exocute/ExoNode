package exonode.clifton.node.entries

import java.io.Serializable

/**
  * Created by #GrowinScala
  *
  * Generic Entry
  */
case class ExoEntry[T <: Serializable](marker: String, payload: T) {

  def setMarker(newMarker: String) = ExoEntry(newMarker, payload)

  def setPayload(newPayload: T) = ExoEntry(marker, newPayload)

  override def toString: String = marker + ":" + payload
}
