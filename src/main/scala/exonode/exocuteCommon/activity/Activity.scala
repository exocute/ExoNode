package exonode.exocuteCommon.activity

import java.io.Serializable

/**
  * Created by #ScalaTeam on 20/12/2016.
  */
trait Activity {

  def process(input: Serializable, params: Vector[String]): Serializable

}
