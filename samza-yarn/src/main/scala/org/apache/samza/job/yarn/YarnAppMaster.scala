/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.job.yarn

import scala.collection.JavaConversions._
import grizzled.slf4j.Logging
import org.apache.hadoop.yarn.client.AMRMClient

/**
 * YARN's API is somewhat clunky. Most implementations just sit in a loop, and
 * poll the resource manager every N seconds (see the distributed shell
 * example). To make life slightly better, Samza separates the polling logic
 * from the application master logic, and we convert synchronous polling calls
 * to callbacks, which are more intuitive when dealing with event based
 * paradigms like YARN.
 *
 * <br/><br/>
 *
 * SamzaAppMaster uses this class to wire up all of Samza's application master
 * listeners.
 */
class YarnAppMaster(pollIntervalMs: Long, listeners: List[YarnAppMasterListener], amClient: AMRMClient) extends Logging {
  var isShutdown = false

  def this(listeners: List[YarnAppMasterListener], amClient: AMRMClient) = this(1000, listeners, amClient)

  def run {
    try {
      listeners.foreach(_.onInit)

      while (!isShutdown && !listeners.map(_.shouldShutdown).reduceLeft(_ || _)) {
        val response = amClient.allocate(0).getAMResponse

        if (response.getReboot) {
          listeners.foreach(_.onReboot)
        }

        listeners.foreach(_.onEventLoop)
        response.getCompletedContainersStatuses.foreach(containerStatus => listeners.foreach(_.onContainerCompleted(containerStatus)))
        response.getAllocatedContainers.foreach(container => listeners.foreach(_.onContainerAllocated(container)))

        try {
          Thread.sleep(pollIntervalMs)
        } catch {
          case e: InterruptedException => {
            isShutdown = true
            info("got interrupt in app master thread, so shutting down")
          }
        }
      }
    } finally {
      listeners.foreach(listener => try {
        listener.onShutdown
      } catch {
        case e: Throwable => warn("Listener %s failed to shutdown." format listener, e)
      })
    }
  }
}
