package exonode.clifton.config

/**
  * Created by #GrowinScala
  */
class BackupConfig {

  protected final val MIN: Long = 60 * 1000
  protected final val HOUR: Long = 60 * MIN

  def BACKUP_DATA_LEASE_TIME: Long = 60 * MIN

  final def RENEW_BACKUP_ENTRIES_TIME: Long = BACKUP_DATA_LEASE_TIME / 3 * 2

  final def MAX_BACKUPS_IN_SPACE: Long = 1 + BACKUP_DATA_LEASE_TIME / RENEW_BACKUP_ENTRIES_TIME

  def BACKUP_TIMEOUT_TIME: Long = 15 * MIN

  final def ANALYSER_CHECK_BACKUP_INFO: Long = BACKUP_TIMEOUT_TIME / 3

  final def SEND_STILL_PROCESSING_TIME: Long = BACKUP_TIMEOUT_TIME / 3

}

object BackupConfig {

  implicit object BackupConfigDefault extends BackupConfig

}

object BackupConfigFast extends BackupConfig {
  override def BACKUP_DATA_LEASE_TIME: Long = 2 * MIN

  override def BACKUP_TIMEOUT_TIME: Long = 1 * MIN
}
