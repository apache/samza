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
package org.apache.samza.zk;

import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.coordinator.CoordinationSessionListener;
import org.apache.samza.coordinator.CoordinationUtils;
import org.apache.samza.coordinator.DistributedDataAccess;
import org.apache.samza.coordinator.DistributedLockWithState;
import org.apache.samza.coordinator.DistributedReadWriteLock;
import org.apache.samza.coordinator.Latch;
import org.apache.samza.coordinator.LeaderElector;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkCoordinationUtils implements CoordinationUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ZkCoordinationUtils.class);

  public final ZkConfig zkConfig;
  public final ZkUtils zkUtils;
  public final String processorIdStr;
  public CoordinationSessionListener listener;

  public ZkCoordinationUtils(String processorId, ZkConfig zkConfig, ZkUtils zkUtils) {
    this.zkConfig = zkConfig;
    this.zkUtils = zkUtils;
    this.processorIdStr = processorId;
    this.listener = null;
  }

  @Override
  public LeaderElector getLeaderElector() {
    return new ZkLeaderElector(processorIdStr, zkUtils);
  }

  @Override
  public Latch getLatch(int size, String latchId) {
    return new ZkProcessorLatch(size, latchId, processorIdStr, zkUtils);
  }

  @Override
  public DistributedLockWithState getLockWithState(String lockId) {
    return new ZkDistributedLock(processorIdStr, zkUtils, lockId);
  }

  public void close() {
    try {
      if (zkUtils != null)
        zkUtils.close();
    } catch (ZkInterruptedException ex) {
      // Swallowing due to occurrence in the last stage of lifecycle(Not actionable).
      LOG.error("Exception in close(): ", ex);
    }
  }

  // TODO - SAMZA-1128 CoordinationService should directly depend on ZkUtils and DebounceTimer
  public ZkUtils getZkUtils() {
    return zkUtils;
  }

  @Override
  public DistributedReadWriteLock getReadWriteLock() {
    return new ZkDistributedReadWriteLock(processorIdStr, zkUtils);
  }

  @Override
  public DistributedDataAccess getDataAccess() {
    return new ZkDistributedDataAccess(zkUtils);
  }

  @Override
  public void setCoordinationSessionListener(CoordinationSessionListener sessionListener) {
    this.listener = sessionListener;
    if (zkUtils != null) {
      zkUtils.getZkClient().subscribeStateChanges(new ZkSessionStateChangedListener());
    }
  }

  /// listener to handle ZK state change events
  class ZkSessionStateChangedListener implements IZkStateListener {
    @Override
    public void handleStateChanged(Watcher.Event.KeeperState state) {
      // TODO: Manasa: nothing much that LAR wants to do here
      LOG.info("Got handleStateChanged event for processor=" + processorIdStr);
    }

    @Override
    public void handleNewSession() {
      LOG.info("Got new session created event for processor=" + processorIdStr);
      listener.handleReconnect();
    }

    @Override
    public void handleSessionEstablishmentError(Throwable error) {
      // TODO: Manasa: nothing much LAR wants to do
      LOG.info("Got handleSessionEstablishmentError for processor=" + processorIdStr, error);
    }
  }
}
