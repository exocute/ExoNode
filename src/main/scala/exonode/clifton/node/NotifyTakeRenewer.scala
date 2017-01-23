/*
 * Copyright (c) 2006-2012 Zink Digital Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package exonode.clifton.node

import com.zink.fly.NotifyHandler
import com.zink.scala.fly.NotifyOps

/**
  * Created by #ScalaTeam on 20/01/2017.
  *
  */
class NotifyTakeRenewer(val space: NotifyOps, val template: AnyRef, val handler: NotifyHandler, val leaseTime: Long) {

  private val renewer = new RenewThread
  private var running = true

  renewer.start()

  def cancel() {
    if (renewer.isAlive)
      renewer.interrupt()
    running = false
  }

  private class RenewThread extends Thread {
    this.setName("NotifyTake Lease Renewer")
    this.setDaemon(true)

    override def run() {
      while (running) {
        try {
          space.notifyTake(template, handler, leaseTime)
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
