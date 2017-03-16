package exonode.clifton.node

/**
  * Created by #GrowinScala
  */
trait Node {

  val nodeId: String

  def finishedProcessing(): Unit

}
