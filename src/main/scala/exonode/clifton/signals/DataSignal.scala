package exonode.clifton.signals

import java.io.Serializable

/**
  * Created by #ScalaTeam on 05-01-2017.
  */
case class DataSignal(to: String, from: String, res: Serializable, injectID: String)