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

package org.apache.samza.clustermanager;

import org.apache.samza.job.CommandBuilder;

import java.util.List;

/**
 * <code>ClusterResourceManager</code> handles communication with a cluster manager
 * and provides updates on events such as resource allocations and
 * completions. Any offer-based resource management system that integrates with Samza
 * will provide an implementation of a ClusterResourceManager API.
 *
 * This class is meant to be used by implementing a CallbackHandler:
 * <pre>
 * {@code
 * class MyCallbackHandler implements ClusterResourceManager.CallbackHandler {
 *   public void onResourcesAvailable(List<SamzaResource> resources) {
 *     [launch a streamprocessor on the resources]
 *   }
 *
 *   public void onResourcesCompleted(List<SamzaResourceStatus> resourceStatus) {
 *     [check for exit code to examine diagnostics, and take actions]
 *   }
 *
 *   public void onError(Throwable error) {
 *     [stop the container process manager]
 *   }
 *
 * }
 * }
 * </pre>
 *
 * The lifecycle of a ClusterResourceManager should be managed similarly to the following:
 *
 * <pre>
 * {@code
 * ClusterResourceManager processManager =
 *     new ClusterResourceManager(callback);
 * processManager.start();
 * [... request resources ...]
 * [... wait for application to complete ...]
 * processManager.stop();
 * }
 * </pre>
 */


/***
 * TODO:
 * 1.Investigate what it means to kill a StreamProcessor, and add it as an API here.
 * 2.Consider an API for Container Process liveness - ie, to be notified when a StreamProcessor
 * joins or leaves the group. Will evolve more as we implement standalone and mesos.
 */

public abstract class ClusterResourceManager {

  protected final Callback clusterManagerCallback;

  public ClusterResourceManager(Callback callback) {
    this.clusterManagerCallback = callback;
  }

  public abstract void start();

  /***
   * Request resources for running container processes
   * @param resourceRequest the resourceRequest being made
   */
  public abstract void requestResources(SamzaResourceRequest resourceRequest);

  /***
   * Remove a previously submitted resource request. The previous resource request may
   * have been submitted to the cluster manager. Even after the remove request, a ContainerProcessManagerCallback
   * implementation must be prepared to receive an allocation for the previous request.
   * This is merely a best effort cancellation.
   *
   * @param request, the resource request that must be cancelled
   */
  public abstract void cancelResourceRequest(SamzaResourceRequest request);


  /***
   * If the app cannot use the resource or wants to give up the resource, it can release them.
   * @param resource the resource to be released
   */
  public abstract void releaseResources(SamzaResource resource);

  /***
   * Requests the launch of a StreamProcessor with the specified context on the resource.
   * @param resource the specified resource
   * @param builder A builder implementation that encapsulates the context for the
   *                StreamProcessor. A builder encapsulates the ID for the processor, the
   *                build environment, the command to execute etc.
   * @throws SamzaContainerLaunchException  when there's an error during the requesting launch.
   *
   */
  public abstract void launchStreamProcessor(SamzaResource resource, CommandBuilder builder) throws SamzaContainerLaunchException;


  public abstract void stop(SamzaApplicationState.SamzaAppStatus status);


  /***
   *Defines a callback interface for interacting with notifications from a ClusterResourceManager
   */
  public interface Callback {

    /***
     * This callback is invoked when there are resources that are to be offered to the application.
     * Often, resources that an app requests may not be available. The application must be prepared
     * to handle callbacks for resources that it did not request.
     * @param resources that are offered to the application
     */
    void onResourcesAvailable(List<SamzaResource> resources);

    /***
     * This callback is invoked when resources are no longer available to the application. A
     * resource could be marked 'completed' in scenarios like - failure of disk on the host,
     * pre-emption of the resource to run another StreamProcessor, exit or termination of the
     * StreamProcessor running in the resource.
     *
     * The SamzaResourceStatus contains diagnostics on why the failure occured
     * @param resources statuses for the resources that were completed.
     */
    void onResourcesCompleted(List<SamzaResourceStatus> resources);

    /***
     * This callback is invoked when there is an error in the ClusterResourceManager. This is
     * guaranteed to be invoked when there is an uncaught exception in any other
     * ClusterResourceManager callbacks.
     * @param e  the underlying Throwable was thrown.
     */
    void onError(Throwable e);
  }
}




