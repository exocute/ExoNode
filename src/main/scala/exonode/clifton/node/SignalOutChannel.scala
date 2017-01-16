package exonode.clifton.node

import com.zink.fly.Fly

/**
  * Created by #ScalaTeam on 20/12/2016.
  */
@deprecated
class SignalOutChannel(marker: String, entryLifeTime: Long = 60 * 1000) extends OutChannel(marker, entryLifeTime) {

  def getSpace: Fly = SpaceCache.getSignalSpace

}