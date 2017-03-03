package exonode.clifton.node.entries

import java.io.Serializable

/**
  * Created by #ScalaTeam on 05-01-2017.
  *
  * DataEntry transports the results of an activity
  * injectID is unique for every inject made. This allows to make joins and forks correctly
  * and to track the process in the graph
  */
case class DataEntry(toAct: String, fromAct: String, injectId: String, orderId: String, data: Option[Serializable]) {

  def setTo(newToAct: String): DataEntry = DataEntry(newToAct, fromAct, injectId, orderId, data)

  def setFrom(newFromAct: String): DataEntry = DataEntry(toAct, newFromAct, injectId, orderId, data)

  def setInjectId(newInjectId: String): DataEntry = DataEntry(toAct, fromAct, newInjectId, orderId, data)

  def setOrderId(newOrderId: String): DataEntry = DataEntry(toAct, fromAct, injectId, newOrderId, data)

  def setData(newData: Option[Serializable]): DataEntry = DataEntry(toAct, fromAct, injectId, orderId, newData)

  def createBackup(): BackupEntry = BackupEntry(toAct, fromAct, injectId, orderId, data)

  def createInfoBackup(): BackupInfoEntry = BackupInfoEntry(toAct, fromAct, orderId, injectId)

}