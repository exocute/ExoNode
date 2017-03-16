package exonode.clifton.node.entries

/**
  * Created by #GrowinScala
  *
  * BackupInfoEntry transports the backup results of an activity
  * injectID is unique for every inject made. This allows to make joins and forks correctly
  * and to track the process in the graph
  */
case class BackupInfoEntry(toAct: String, fromAct: String, injectId: String, orderId: String) {

  def createTemplate(): BackupEntry = BackupEntry(toAct, fromAct, injectId, orderId, null)

}