package exonode.clifton.node.entries

import java.io.Serializable

/**
  * Created by #ScalaTeam on 05-01-2017.
  *
  * DataEntry transports the results of an activity
  * injectID is unique for every inject made. This allows to make joins and forks correctly
  * and to track the process in the graph
  */
case class DataEntry(toAct: String, fromAct: String, injectId: String, data: Serializable) {

  def setTo(newToAct: String): DataEntry = DataEntry(newToAct, fromAct, injectId, data)

  def setFrom(newFromAct: String): DataEntry = DataEntry(toAct, newFromAct, injectId, data)

  def setInjectId(newInjectId: String): DataEntry = DataEntry(toAct, fromAct, newInjectId, data)

  def setData(newData: Serializable): DataEntry = DataEntry(toAct, fromAct, injectId, newData)

  def createBackup(): BackupEntry = BackupEntry(toAct, fromAct, injectId, data)

  def createInfoBackup(): BackupInfoEntry = BackupInfoEntry(toAct, fromAct, injectId)

}