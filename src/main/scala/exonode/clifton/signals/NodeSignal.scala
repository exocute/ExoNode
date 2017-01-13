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

trait NodeSignal

case object KillSignal extends NodeSignal

case object KillGracefullSignal extends NodeSignal
