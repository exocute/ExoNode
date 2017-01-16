package exonode.clifton.node

import java.io.Serializable

/**
  * Created by #ScalaTeam on 05-01-2017.
  *
  * DataEntry transports the results of an activity
  * injectID is unique for every inject made. This allows to make joins and forks correctly
  * and to track the process in the graph
  */
class DataEntry(val toAct: String, val fromAct: String, val injectId: String, val data: Serializable) {

  def this() = this(null, null, null, null)

  def setTo(newToAct: String): DataEntry = new DataEntry(newToAct, fromAct, injectId, data)

  def setFrom(newFromAct: String): DataEntry = new DataEntry(toAct, newFromAct, injectId, data)

  def setInjectId(newInjectId: String): DataEntry = new DataEntry(toAct, fromAct, newInjectId, data)

}