package exonode.clifton.node.work

/**
  * Created by #GrowinScala
  */
sealed trait WorkType {
  def hasWork: Boolean

  def activity: ActivityWorker
}

case object NoWork extends WorkType {
  override def hasWork = false

  override def activity = throw new NoSuchElementException("NoWork.activity")
}

case class ConsecutiveWork(activity: ActivityWorker) extends WorkType {
  override def hasWork = true
}

case class JoinWork(activity: ActivityWorker, actsFrom: Vector[String]) extends WorkType {
  override def hasWork = true
}