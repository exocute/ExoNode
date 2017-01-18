package exonode.clifton.node.work

import exonode.clifton.node.DataEntry

/**
  * Created by #ScalaTeam on 18-01-2017.
  */
trait Worker {
  def sendInput(activityWorker: ActivityWorker, input: Vector[DataEntry]): Unit
}
