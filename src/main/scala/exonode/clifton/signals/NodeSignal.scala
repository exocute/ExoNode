package exonode.clifton.signals

/**
  * Created by #GrowinScala
  */

sealed trait NodeSignal extends Serializable

//if a nodes receives a KillSignal immediately aborts and die
case object KillSignal extends NodeSignal

//if a node receives a KillGracefullSignal waits if something are being processed and then aborts
case object KillGracefullSignal extends NodeSignal
