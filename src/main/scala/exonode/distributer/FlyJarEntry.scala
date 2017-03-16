package exonode.distributer

/**
  * Created by #GrowinScala
  *
  * It allows us to save in the space for every jarName the correspondent bytes
  */
case class FlyJarEntry(fileName: String, bytes: Array[Byte]) {

  override def toString: String = "Filename [" + fileName + "] bytes [" + bytes + "]"
}
