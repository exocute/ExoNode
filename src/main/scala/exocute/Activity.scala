package exocute

import java.io.Serializable

/**
  * Created by #GrowinScala
  */
trait Activity {

  def process(input: Serializable, params: Vector[String]): Serializable

}
