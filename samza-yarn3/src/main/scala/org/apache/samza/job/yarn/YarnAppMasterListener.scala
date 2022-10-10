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
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.ContainerStatus

/**
 * Classes that wish to listen to callback events from YarnAppMaster must
 * implement this trait.
 */
trait YarnAppMasterListener {
  /**
   * If true, YarnAppMaster will cease to poll the RM, and call onShutdown for
   * all listeners.
   */
  def shouldShutdown: Boolean = false

  /**
   * Invoked by YarnAppMaster once per listener, before entering the RM polling
   * event loop.
   */
  def onInit() {}

  /**
   * Invoked whenever the RM responds with a reboot request. Usually, reboots
   * are triggered by the YARN RM when its state gets out of sync with the
   * application master (usually the result of restarting the RM).
   * YarnAppMaster does not actually restart anything. It is up to one or more
   * listeners to trigger a failure, or shutdown.
   */
  def onReboot() {}

  /**
   * Signifies that the YarnAppMaster has exited the RM polling event loop, and
   * is about to exit.
   */
  def onShutdown() {}

  /**
   * Whenever the RM allocates a container for the application master, this
   * callback is invoked (once per container).
   */
  def onContainerAllocated(container: Container) {}

  /**
   * Whenever a container completes (either failure, or success), this callback
   * will be invoked.
   */
  def onContainerCompleted(containerStatus: ContainerStatus) {}

}
