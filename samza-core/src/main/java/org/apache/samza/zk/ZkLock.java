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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.coordinator.Lock;
import org.apache.samza.coordinator.LockListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Lock primitive for Zookeeper.
 */
public class ZkLock implements Lock {

  public static final Logger LOG = LoggerFactory.getLogger(ZkLock.class);
  private final static String LOCK_PATH = "lock";
  private final ZkConfig zkConfig;
  private final ZkUtils zkUtils;
  private final String lockPath;
  private final String participantId;
  private final ZkKeyBuilder keyBuilder;
  private final Random random = new Random();
  private String nodePath = null;
  private LockListener zkLockListener = null;
  private AtomicBoolean hasLock;

  public ZkLock(String participantId, ZkUtils zkUtils, ZkConfig zkConfig) {
    this.zkConfig = zkConfig;
    this.zkUtils = zkUtils;
    this.participantId = participantId;
    this.keyBuilder = this.zkUtils.getKeyBuilder();
    lockPath = String.format("%s/%s", keyBuilder.getRootPath(), LOCK_PATH);
    zkUtils.makeSurePersistentPathsExists(new String[] {lockPath});
    this.hasLock = new AtomicBoolean(false);
  }

  /**
   * Tries to acquire a lock in order to create intermediate streams. On failure to acquire lock, it keeps trying until the lock times out.
   * Creates a sequential ephemeral node to acquire the lock. If the path of this node has the lowest sequence number, the processor has acquired the lock.
   */
  @Override
  public boolean lock() {
    nodePath = zkUtils.getZkClient().createEphemeralSequential(lockPath + "/", participantId);

    //Start timer for timeout
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("ZkLock-%d").build());
    scheduler.schedule(() -> {
        if (!hasLock.get()) {
          throw new SamzaException("Timed out while acquiring lock for creating intermediate streams.");
        }
      }, zkConfig.getZkSessionTimeoutMs(), TimeUnit.MILLISECONDS);

    while (!hasLock.get()) {
      List<String> children = zkUtils.getZkClient().getChildren(lockPath);
      int index = children.indexOf(ZkKeyBuilder.parseIdFromPath(nodePath));

      if (children.size() == 0 || index == -1) {
        throw new SamzaException("Looks like we are no longer connected to Zk. Need to reconnect!");
      }
      // Acquires lock when the node has the lowest sequence number and returns.
      if (index == 0) {
        hasLock.set(true);
        LOG.info("Acquired lock for participant id: {}", participantId);
        return true;
      } else {
        // Keep trying to acquire the lock
        try {
          Thread.sleep(random.nextInt(1000));
        } catch (InterruptedException e) {
          Thread.interrupted();
        }
        LOG.info("Trying to acquire lock again...");
      }
    }
    return hasLock.get();
  }

  /**
   * Unlocks, by deleting the ephemeral sequential node created to acquire the lock.
   */
  @Override
  public void unlock() {
    if (nodePath != null) {
      zkUtils.getZkClient().delete(nodePath);
      hasLock.set(false);
      nodePath = null;
      LOG.info("Ephemeral lock node deleted. Unlocked!");
    } else {
      LOG.info("Ephemeral lock node doesn't exist");
    }
  }

  @Override
  public boolean hasLock() {
    return hasLock.get();
  }

  @Override
  public void setLockListener(LockListener listener) {
    this.zkLockListener = listener;
  }

}