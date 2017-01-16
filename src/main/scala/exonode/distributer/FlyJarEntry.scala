package exonode.distributer

/**
  * Created by #ScalaTeam on 20/12/2016.
  *
  * It allows us to save in the space for every jarName the correspondent bytes
  */
class FlyJarEntry(var fileName: String, var bytes: Array[Byte]) {
  def this() = this("", null)

  override def toString: String = "Filename [" + fileName + "] bytes [" + bytes + "]"
}
