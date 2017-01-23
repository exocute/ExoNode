package exonode.clifton.node

import com.zink.fly.NotifyHandler
import com.zink.scala.fly.NotifyOps

/**
  * Created by #ScalaTeam on 20/01/2017.
  *
  */
class NotifyWriteRenewer(val space: NotifyOps, val template: AnyRef, val handler: NotifyHandler, val leaseTime: Long) {

  private val renewer = new RenewThread
  private var running = true

  renewer.start()

  def cancel() {
    if (renewer.isAlive)
      renewer.interrupt()
    running = false
  }

  private class RenewThread extends Thread {
    this.setName("NotifyWrite Lease Renewer")
    this.setDaemon(true)

    override def run() {
      while (running) {
        try {
          space.notifyWrite(template, handler, leaseTime)
          Thread.sleep(leaseTime)
        } catch {
          case ex: InterruptedException => {
            running = false
          }
        }
      }
    }
  }

}
