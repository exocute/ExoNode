package exonode.clifton.node.work

/**
  * Created by #GrowinScala
  */
trait BusyWorking {
  def threadIsBusy: Boolean

  def cancelThread(): Unit = ()
}
