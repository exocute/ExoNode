package exonode.clifton.node.work

import java.io.Serializable

import exocute.Activity
import exonode.clifton.signals.ActivityType

class ActivityWorker(val id: String, val actType: ActivityType, activity: Activity,
                     params: Vector[String], val acsTo: Vector[String]) {
  def process(input: Serializable): Serializable = activity.process(input, params)
}