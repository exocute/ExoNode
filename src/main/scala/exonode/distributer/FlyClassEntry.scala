package exonode.distributer

/**
  * Created by #ScalaTeam on 20/12/2016.
  *
  * It allows us to save in space for every className the correspondent jarName
  */
class FlyClassEntry(var className: String, var jarName: String) {
  def this() = this("", "")

  override def toString: String = "Filename [" + className + "] bytes [" + jarName + "]"
}
