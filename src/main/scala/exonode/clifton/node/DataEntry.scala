package exonode.clifton.node

import java.io.Serializable

/**
  * Created by #ScalaTeam on 05-01-2017.
  */
class DataEntry(val toAct: String, val fromAct: String, val injectId: String, val data: Serializable) {

  def this() = this(null, null, null, null)

  def setTo(newToAct: String): DataEntry = new DataEntry(newToAct, fromAct, injectId, data)

  def setFrom(newFromAct: String): DataEntry = new DataEntry(toAct, newFromAct, injectId, data)

  def setInjectId(newInjectId: String): DataEntry = new DataEntry(toAct, fromAct, newInjectId, data)

}