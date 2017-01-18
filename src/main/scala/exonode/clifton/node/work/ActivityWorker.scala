package exonode.clifton.node.work

import java.io.Serializable
import exonode.exocuteCommon.activity.Activity

class ActivityWorker(val id: String, activity: Activity, params: Vector[String], val acsTo: Vector[String]) {
  def process(input: Serializable): Serializable = activity.process(input, params)
}