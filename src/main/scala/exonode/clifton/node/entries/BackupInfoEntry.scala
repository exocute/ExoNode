package exonode.clifton.node.entries

/**
  * Created by #ScalaTeam on 05-01-2017.
  *
  * BackupInfoEntry transports the backup results of an activity
  * injectID is unique for every inject made. This allows to make joins and forks correctly
  * and to track the process in the graph
  */
case class BackupInfoEntry(toAct: String, fromAct: String, injectId: String) {

  def setTo(newToAct: String): BackupInfoEntry = BackupInfoEntry(newToAct, fromAct, injectId)

  def setFrom(newFromAct: String): BackupInfoEntry = BackupInfoEntry(toAct, newFromAct, injectId)

  def setInjectId(newInjectId: String): BackupInfoEntry = BackupInfoEntry(toAct, fromAct, newInjectId)

  def createTemplate(): BackupEntry = BackupEntry(toAct, fromAct, injectId, null)

}