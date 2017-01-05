package exonode.clifton.node

import com.zink.fly.FlyPrime

/**
  * Created by #ScalaTeam on 20/12/2016.
  */
class SignalOutChannel(marker: String, entryLifeTime: Long = 60 * 1000) extends OutChannel(marker, entryLifeTime) {

  def getSpace: FlyPrime = SpaceCache.getSignalSpace

}