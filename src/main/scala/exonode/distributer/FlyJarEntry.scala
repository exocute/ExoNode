package exonode.distributer

/**
  * Created by #ScalaTeam on 20/12/2016.
  */
class FlyJarEntry(var fileName: String, var bytes: Array[Byte]) {
  def this() = this("", null)

  override def toString: String = "Filename [" + fileName + "] bytes [" + bytes + "]"
}
