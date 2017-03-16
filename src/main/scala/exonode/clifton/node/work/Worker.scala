package exonode.clifton.node.work

import exonode.clifton.node.entries.DataEntry

/**
  * Created by #GrowinScala
  */
trait Worker {
  def sendInput(activityWorker: ActivityWorker, input: Vector[DataEntry]): Unit
}
