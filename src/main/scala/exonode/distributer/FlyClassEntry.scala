package exonode.distributer

/**
  * Created by #ScalaTeam on 20/12/2016.
  */
class FlyClassEntry(var className: String, var jarName: String) {
  def this() = this("", "")

  override def toString: String = "Filename [" + className + "] bytes [" + jarName + "]"
}
