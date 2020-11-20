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

import java.util.Map;
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
 *     [launch a processor on the resources]
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
 * 1.Investigate what it means to kill a processor, and add it as an API here.
 * 2.Consider an API for processor liveness - ie, to be notified when a processor
 * joins or leaves the group. Will evolve more as we implement standalone and mesos.
 */

public abstract class ClusterResourceManager {

  protected final Callback clusterManagerCallback;

  public ClusterResourceManager(Callback callback) {
    this.clusterManagerCallback = callback;
  }

  public abstract void start();

  /***
   * Request resources for running processors
   * @param resourceRequest the resourceRequest being made
   */
  public abstract void requestResources(SamzaResourceRequest resourceRequest);

  /**
   * Get the node to fault domain map from the cluster resource manager.
   * @return A map of the nodes to the fault domain they reside in.
   */
  public abstract Map<String, String> getNodeToFaultDomainMap();

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
   * Requests the launch of a processor with the specified context on the resource asynchronously.
   *
   * <p>
   *   Either {@link Callback#onStreamProcessorLaunchSuccess(SamzaResource)} or
   *   {@link Callback#onStreamProcessorLaunchFailure(SamzaResource, Throwable)} will be invoked
   *   to indicate the result of this operation.
   * </p>
   *
   * @param resource the specified resource
   * @param builder A builder implementation that encapsulates the context for the
   *                processor. A builder encapsulates the ID for the processor, the
   *                build environment, the command to execute etc.
   *
   */
  public abstract void launchStreamProcessor(SamzaResource resource, CommandBuilder builder);

  /**
   * Requests the stopping of a processor, identified by the given resource.
   * {@link Callback#onResourcesCompleted(List)} will be invoked to indicate the completion of this operation.
   *
   * @param resource the resource being used for the processor.
   */
  public abstract void stopStreamProcessor(SamzaResource resource);


  public abstract void stop(SamzaApplicationState.SamzaAppStatus status);

  /**
   * Checks if the allocated resource is expired. If the {@link ClusterResourceManager} does not have a
   * concept of expired allocated resource we assume allocated resources never expire
   * @param resource allocated resource
   * @return if the allocated resource is expired
   */
  public boolean isResourceExpired(SamzaResource resource) {
    return false;
  }

  /***
   * Defines a callback interface for interacting with notifications from a ClusterResourceManager
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
     * pre-emption of the resource to run another processor, exit or termination of the
     * processor running in the resource.
     *
     * The SamzaResourceStatus contains diagnostics on why the failure occured
     * @param resources statuses for the resources that were completed.
     */
    void onResourcesCompleted(List<SamzaResourceStatus> resources);


    /**
     * Callback invoked when the launch request for a processor on the {@link SamzaResource} is successful.
     * @param resource the resource on which the processor is launched
     */
    void onStreamProcessorLaunchSuccess(SamzaResource resource);

    /**
     * Callback invoked when there is a failure in launching a processor on the provided {@link SamzaResource}.
     * @param resource the resource on which the processor was submitted for launching
     * @param t the error in launching the processor
     */
    void onStreamProcessorLaunchFailure(SamzaResource resource, Throwable t);

    /**
     * Callback invoked when there is a failure in stopping a processor on the provided {@link SamzaResource}.
     * @param resource the resource on which the processor was running
     * @param t the error in stopping the processor
     */
    void onStreamProcessorStopFailure(SamzaResource resource, Throwable t);

    /***
     * This callback is invoked when there is an error in the ClusterResourceManager. This is
     * guaranteed to be invoked when there is an uncaught exception in any other
     * ClusterResourceManager callbacks.
     * @param e  the underlying Throwable was thrown.
     */
    void onError(Throwable e);
  }
}




