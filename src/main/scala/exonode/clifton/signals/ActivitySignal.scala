package exonode.clifton.signals

/**
  * Created by #ScalaTeam on 20-12-2016.
  *
  * It allows to save in the space the graph representation
  */
class ActivitySignal(var name: String, var params: Vector[String], var inMarkers: Vector[String], var outMarkers: Vector[String]) extends Serializable {

  def this() = this("", Vector[String](), Vector[String](), Vector[String]())

  override def toString: String = {
    val ret = new StringBuilder(64)
    ret.append("Activity :" + name + "\n"
      + "Params " + params.mkString(", ") + "\n"
      + "In  " + inMarkers.mkString(", ") + "\n"
      + "Out " + outMarkers.mkString(", ") + "\n")
    ret.toString
  }
}
