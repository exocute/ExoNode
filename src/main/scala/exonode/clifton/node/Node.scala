package exonode.clifton.node

/**
  * Created by #ScalaTeam on 14-02-2017.
  */
trait Node {

  val nodeId: String

  def finishedProcessing(): Unit

}
