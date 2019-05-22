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

import java.time.Duration;
import java.util.List;
import org.apache.samza.SamzaException;
import org.apache.samza.coordinator.DistributedLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Distributed lock primitive for Zookeeper.
 */
public class ZkDistributedLock implements DistributedLock {

  public static final Logger LOG = LoggerFactory.getLogger(ZkDistributedLock.class);
  private static final String STATE_INITED = "sate_initialized";
  private final ZkUtils zkUtils;
  private final String lockPath;
  private final String participantId;
  private final ZkKeyBuilder keyBuilder;
  private String nodePath = null;
  private Object mutex;

  public ZkDistributedLock(String participantId, ZkUtils zkUtils, String lockId) {
    this.zkUtils = zkUtils;
    this.participantId = participantId;
    this.keyBuilder = zkUtils.getKeyBuilder();
    lockPath = String.format("%s/lock_%s", keyBuilder.getRootPath(), lockId);
    zkUtils.validatePaths(new String[] {lockPath});
    mutex = new Object();
    zkUtils.getZkClient().subscribeChildChanges(lockPath, new ParticipantChangeHandler(zkUtils));
  }

  /**
   * Tries to acquire a lock in order to create intermediate streams. On failure to acquire lock, it keeps trying until the lock times out.
   * Creates a sequential ephemeral node to acquire the lock. If the path of this node has the lowest sequence number, the processor has acquired the lock.
   * @param timeout Duration of lock acquiring timeout.
   * @return true if lock is acquired successfully else returns false if failed to acquire within timeout
   */
  @Override
  public boolean lock(Duration timeout) {

    nodePath = zkUtils.getZkClient().createEphemeralSequential(lockPath + "/", participantId);

    //Start timer for timeout
    long startTime = System.currentTimeMillis();
    long lockTimeout = timeout.toMillis();

    while ((System.currentTimeMillis() - startTime) < lockTimeout) {
      synchronized (mutex) {
        List<String> children = zkUtils.getZkClient().getChildren(lockPath);
        int index = children.indexOf(ZkKeyBuilder.parseIdFromPath(nodePath));

        if (children.size() == 0 || index == -1) {
          throw new SamzaException("Looks like we are no longer connected to Zk. Need to reconnect!");
        }
        // Acquires lock when the node has the lowest sequence number and returns.
        if (index == 0) {
          LOG.info("Acquired lock for participant id: {}", participantId);
          return true;
        } else {
          try {
            mutex.wait(lockTimeout);
          } catch (InterruptedException e) {
            Thread.interrupted();
          }
          LOG.info("Trying to acquire lock again...");
        }
      }
    }
    LOG.info("Failed to acquire lock within {} milliseconds.", lockTimeout);
    return false;
  }

  /**
   * Unlocks, by deleting the ephemeral sequential node created to acquire the lock.
   */
  @Override
  public void unlock() {

    if (nodePath != null) {
      zkUtils.getZkClient().delete(nodePath);
      nodePath = null;
      LOG.info("Ephemeral lock node deleted. Unlocked!");
    } else {
      LOG.warn("Ephemeral lock node you want to delete doesn't exist");
    }
  }

  /**
   * Listener for changes in children of LOCK
   * children are the ephemeral nodes created to acquire the lock
   */
  class ParticipantChangeHandler extends ZkUtils.GenerationAwareZkChildListener {

    public ParticipantChangeHandler(ZkUtils zkUtils) {
      super(zkUtils, "ParticipantChangeHandler");
    }

    // Called when the children of the given path changed.
    @Override
    public void doHandleChildChange(String parentPath, List<String> currentChildren)
        throws Exception {
      synchronized (mutex) {
        if (currentChildren == null) {
          LOG.warn("handleChildChange on path " + parentPath + " was invoked with NULL list of children");
        } else {
          LOG.info("ParticipantChangeHandler::handleChildChange - Path: {} Current Children: {} ", parentPath, currentChildren);
          mutex.notify();
        }
      }
    }
  }
}