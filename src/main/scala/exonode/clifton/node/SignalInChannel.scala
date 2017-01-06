package exonode.clifton.node

import com.zink.fly.FlyPrime

/**
  * Created by #ScalaTeam on 20/12/2016.
  */
class SignalInChannel(marker: String) extends InChannel(marker) {

  def getSpace: FlyPrime = SpaceCache.getSignalSpace

}
