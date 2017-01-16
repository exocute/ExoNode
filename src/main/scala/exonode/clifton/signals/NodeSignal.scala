package exonode.clifton.signals

/**
  * Created by #ScalaTeam on 30-12-2016.
  */

trait NodeSignal

//if a nodes receives a KillSignal immediately aborts and die
case object KillSignal extends NodeSignal


//if a node receives a KillGracefullSignal waits if something are being processed and then aborts
case object KillGracefullSignal extends NodeSignal
