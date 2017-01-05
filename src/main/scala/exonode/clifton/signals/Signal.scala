package exonode.clifton.signals

/**
  * Created by #ScalaTeam on 30-12-2016.
  */

/*
  BOOT,
  ACTIVITY_SIGNAL,
  LOG_SIGNAL,
  KILL_SIGNAL,
  FIRST_JOB,
  CONSECUTIVE_JOB,
  BUSY_PROCESSING,
  ACTIVITY_TIMEOUT,
  NODE_TIMEOUT,
  NODE_DEAD
*/

trait Signal

case class BootSignal(id: String) extends Signal

case object KillSignal extends Signal

case class ProcessActivitySignal(actName: String) extends Signal
