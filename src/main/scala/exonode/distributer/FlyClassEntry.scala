package exonode.distributer

/**
  * Created by #GrowinScala
  *
  * It allows us to save in space for every className the correspondent jarName
  */
case class FlyClassEntry(className: String, jarName: String) {

  override def toString: String = "Filename [" + className + "] bytes [" + jarName + "]"
}
