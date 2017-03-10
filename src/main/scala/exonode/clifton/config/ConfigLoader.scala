package exonode.clifton.config

import java.io.File

import scala.util.{Success, Try}

/**
  * Created by #ScalaTeam on 09-02-2017.
  */
object ConfigLoader {

  private val CONFIG_FILE: String = "config.ini"
  private val configConstants: Map[String, Long] = loadConfigurationFile()

  def getOrDefault(constantName: String, defaultValue: Long): Long =
    configConstants.getOrElse(constantName, defaultValue)

  def loadConfigurationFile(): Map[String, Long] = {
    if (!new File(CONFIG_FILE).exists())
      Map()
    else {
      val config = clearComments(scala.io.Source.fromFile(CONFIG_FILE).mkString).split('\n')
      config.foldLeft(Map[String, Long]()) {
        (map, line) =>
          if (line.contains("=")) {
            val entry = Try {
              val sign = line.indexOf("=")
              val (constant, time) = line.splitAt(sign)
              constant.trim -> time.substring(1).trim.toLong
            }
            entry match {
              case Success(value) => map + value
              case _ => map
            }
          } else {
            map
          }
      }
    }
  }

  private def clearComments(file: String): String = {
    file.split("\n").map(str => {
      val index = str.indexOf("//")
      if (index == -1) str
      else str.substring(0, index)
    }).map(str => str.filterNot(_ == '\r')).mkString("\n")
  }

}
