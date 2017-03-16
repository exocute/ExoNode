package exonode.clifton.signals

/**
  * Created by #GrowinScala
  *
  * It allows to save in the space the graph representation
  */
case class ActivitySignal(name: String, actType: ActivityType, params: Vector[String], inMarkers: Vector[String], outMarkers: Vector[String]) extends Serializable {

  def this() = this(null, null, null, null, null)

  override def toString: String = {
    val ret = new StringBuilder(64)
    ret.append("Activity :" + name + "\n"
      + "Params " + params.mkString(", ") + "\n"
      + "In  " + inMarkers.mkString(", ") + "\n"
      + "Out " + outMarkers.mkString(", ") + "\n")
    ret.toString
  }
}
