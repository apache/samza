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
package org.apache.samza.drain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.apache.samza.metadatastore.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DrainMonitor is intended to monitor the MetadataStore for {@link DrainNotification} and invoke
 * the {@link DrainCallback}.
 * */
public class DrainMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(DrainMonitor.class);

  /**
   * Describes the state of the monitor.
   * */
  public enum State {
    /**
     * Initial state when DrainMonitor is not polling for DrainNotifications.
     * */
    INIT,
    /**
     * When Drain Monitor is started, it moves from INIT to RUNNING state and starts polling
     * for Drain Notifications.
     * */
    RUNNING,
    /**
     * Indicates that the Drain Monitor is stopped. The DrainMonitor could have been explicitly stopped or stopped on
     * its own if a DrainNotification was encountered.
     * */
    STOPPED
  }

  private static final int POLLING_INTERVAL_MILLIS = 60_000;
  private static final int INITIAL_POLL_DELAY_MILLIS = 0;

  private final ScheduledExecutorService schedulerService =
      Executors.newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder()
              .setNameFormat("Samza DrainMonitor Thread-%d")
              .setDaemon(true)
              .build());
  private final String appRunId;
  private final long pollingIntervalMillis;
  private final NamespaceAwareCoordinatorStreamStore drainMetadataStore;
  // Used to guard write access to state.
  private final Object lock = new Object();

  @GuardedBy("lock")
  private State state = State.INIT;
  private DrainCallback callback;

  public DrainMonitor(MetadataStore metadataStore, Config config) {
    this(metadataStore, config, POLLING_INTERVAL_MILLIS);
  }

  public DrainMonitor(MetadataStore metadataStore, Config config, long pollingIntervalMillis) {
    Preconditions.checkNotNull(metadataStore, "MetadataStore parameter cannot be null.");
    Preconditions.checkNotNull(config, "Config parameter cannot be null.");
    Preconditions.checkArgument(pollingIntervalMillis > 0,
        String.format("Polling interval specified is %d ms. It should be greater than 0.", pollingIntervalMillis));
    this.drainMetadataStore =
        new NamespaceAwareCoordinatorStreamStore(metadataStore, DrainUtils.DRAIN_METADATA_STORE_NAMESPACE);
    ApplicationConfig applicationConfig = new ApplicationConfig(config);
    this.appRunId = applicationConfig.getRunId();
    this.pollingIntervalMillis = pollingIntervalMillis;
  }

  /**
   * Starts the DrainMonitor.
   * */
  public void start() {
    Preconditions.checkState(callback != null,
        "Drain Callback needs to be set using registerCallback(callback) prior to starting the DrainManager.");
    synchronized (lock) {
      switch (state) {
        case INIT:
          if (shouldDrain(drainMetadataStore, appRunId)) {
            /*
             * Prior to starting the periodic polling, we are doing a one-time check on the calling(container main) thread
             * to see if DrainNotification is present in the metadata store for the current deployment.
             * This check is to deal with the case where a container might have re-started during Drain.
             * If yes, we will set the container to drain mode to prevent it from processing any new messages. This will
             * in-turn guarantee that intermediate Drain control messages from the previous incarnation of the container are
             * processed and there are no duplicate intermediate control messages for the same deployment.
             * */
            LOG.info("Found DrainNotification message on container start. Skipping poll of DrainNotifications.");
            callback.onDrain();
          } else {
            state = State.RUNNING;
            schedulerService.scheduleAtFixedRate(() -> {
              if (shouldDrain(drainMetadataStore, appRunId)) {
                LOG.info("Received Drain Notification for deployment: {}", appRunId);
                stop();
                callback.onDrain();
              }
            }, INITIAL_POLL_DELAY_MILLIS, pollingIntervalMillis, TimeUnit.MILLISECONDS);
            LOG.info("Started DrainMonitor.");
          }
          break;
        case RUNNING:
        case STOPPED:
          LOG.info("Cannot call start() on the DrainMonitor when it is in {} state.", state);
          break;
      }
    }
  }

  /**
   * Stops the DrainMonitor.
   * */
  public void stop() {
    synchronized (lock) {
      switch (state) {
        case RUNNING:
          schedulerService.shutdownNow();
          state = State.STOPPED;
          LOG.info("Stopped DrainMonitor.");
          break;
        case INIT:
        case STOPPED:
          LOG.info("Cannot stop DrainMonitor as it is not running. State: {}.", state);
          break;
      }
    }
  }

  /**
   * Register a callback to be executed when DrainNotification is encountered.
   *
   * @param callback the callback to register.
   * @return Returns {@code true} if registration was successful and {@code false} if not.
   * Registration can fail it the DrainMonitor is stopped or a callback is already registered.
   * */
  public boolean registerDrainCallback(DrainCallback callback) {
    Preconditions.checkNotNull(callback);

    switch (state) {
      case RUNNING:
      case STOPPED:
        LOG.warn("Cannot register callback when it is in {} state. Please register callback before calling start "
            + "on DrainMonitor.", state);
        return false;
      case INIT:
        if (this.callback != null) {
          LOG.warn("Cannot register callback as a callback is already registered.");
          return false;
        }
        this.callback = callback;
        return true;
      default:
        return false;
    }
  }

  /**
   * Get the current state of the DrainMonitor.
   * */
  @VisibleForTesting
  State getState() {
    return state;
  }

  /**
   * Callback for any action to executed by DrainMonitor implementations once Drain is encountered.
   * Registered using {@link #registerDrainCallback(DrainCallback)}.
   * */
  public interface DrainCallback {
    void onDrain();
  }

  /**
   * One time check check to see if there are any DrainNotification messages available in the
   * metadata store for the current deployment.
   * */
  static boolean shouldDrain(NamespaceAwareCoordinatorStreamStore drainMetadataStore, String deploymentId) {
    final Optional<List<DrainNotification>> drainNotifications = readDrainNotificationMessages(drainMetadataStore);
    if (drainNotifications.isPresent()) {
      final ImmutableList<DrainNotification> filteredDrainNotifications = drainNotifications.get()
          .stream()
          .filter(notification -> deploymentId.equals(notification.getDeploymentId()))
          .collect(ImmutableList.toImmutableList());
      return !filteredDrainNotifications.isEmpty();
    }
    return false;
  }

  /**
   * Reads all DrainNotification messages from the metadata store.
   * */
  private static Optional<List<DrainNotification>> readDrainNotificationMessages(NamespaceAwareCoordinatorStreamStore
      drainMetadataStore) {
    final ObjectMapper objectMapper = DrainNotificationObjectMapper.getObjectMapper();
    final ImmutableList<DrainNotification> drainNotifications = drainMetadataStore.all()
        .values()
        .stream()
        .map(bytes -> {
          try {
            return objectMapper.readValue(bytes, DrainNotification.class);
          } catch (IOException e) {
            LOG.error("Unable to deserialize DrainNotification from the metadata store", e);
            throw new SamzaException(e);
          }
        })
        .collect(ImmutableList.toImmutableList());
    return drainNotifications.size() > 0
        ? Optional.of(drainNotifications)
        : Optional.empty();
  }
}
