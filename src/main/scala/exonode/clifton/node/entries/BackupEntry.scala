package exonode.clifton.node.entries

import java.io.Serializable

/**
  * Created by #ScalaTeam on 05-01-2017.
  *
  * BackupEntry transports the backup results of an activity
  * injectID is unique for every inject made. This allows to make joins and forks correctly
  * and to track the process in the graph
  */
case class BackupEntry(toAct: String, fromAct: String, injectId: String, orderId: String, data: Option[Serializable]) {

  //  def setTo(newToAct: String): BackupEntry = BackupEntry(newToAct, fromAct, injectId, data)
  //
  //  def setFrom(newFromAct: String): BackupEntry = BackupEntry(toAct, newFromAct, injectId, data)
  //
  //  def setInjectId(newInjectId: String): BackupEntry = BackupEntry(toAct, fromAct, newInjectId, data)

  //  def setData(newData: Serializable): BackupEntry = BackupEntry(toAct, fromAct, injectId, newData)

  def createDataEntry(): DataEntry = DataEntry(toAct, fromAct, injectId, orderId, data)

}