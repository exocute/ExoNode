package exonode.clifton.node.entries

import java.io.Serializable

/**
  * Created by #GrowinScala
  *
  * BackupEntry transports the backup results of an activity
  * injectID is unique for every inject made. This allows to make joins and forks correctly
  * and to track the process in the graph
  */
case class BackupEntry(toAct: String, fromAct: String, injectId: String, orderId: String, data: Option[Serializable]) {

  def createDataEntry(): DataEntry = DataEntry(toAct, fromAct, injectId, orderId, data)

}