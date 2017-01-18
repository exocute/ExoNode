package exonode.distributer

/**
  * Created by #ScalaTeam on 20/12/2016.
  *
  * It allows us to save in space for every className the correspondent jarName
  */
case class FlyClassEntry(className: String, jarName: String) {

  override def toString: String = "Filename [" + className + "] bytes [" + jarName + "]"
}
