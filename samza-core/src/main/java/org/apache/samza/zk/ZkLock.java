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

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.samza.SamzaException;
import org.apache.samza.coordinator.Lock;
import org.apache.samza.coordinator.LockListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkLock implements Lock {

  public static final Logger LOG = LoggerFactory.getLogger(ZkLeaderElector.class);
  private final ZkUtils zkUtils;
  private final String lockPath;
  private final String participantId;
  private String currentSubscription = null;
  private final ZkKeyBuilder keyBuilder;
  private final IZkDataListener previousProcessorChangeListener;
  private final Random random = new Random();
  private String nodePath = null;
  private LockListener zkLockListener = null;
  private AtomicBoolean hasLock;
  private final static String LOCK_PATH = "lock";

  public ZkLock(String participantId, ZkUtils zkUtils) {
    this.zkUtils = zkUtils;
    this.participantId = participantId;
    this.keyBuilder = this.zkUtils.getKeyBuilder();
    this.previousProcessorChangeListener = new PreviousLockProcessorChangeListener();
    lockPath = String.format("%s/%s", keyBuilder.getRootPath(), LOCK_PATH);
    zkUtils.makeSurePersistentPathsExists(new String[] {lockPath});
    this.hasLock = new AtomicBoolean(false);
  }

  /**
   * Create a sequential ephemeral node to acquire the lock. If the path of this node has the lowest sequence number, the processor has acquired the lock.
   */
  @Override
  public void lock() {
    try {
      nodePath = zkUtils.getZkClient().createEphemeralSequential(lockPath + "/", participantId);
    } catch (Exception e) {
      zkLockListener.onError();
    }
    List<String> children = zkUtils.getZkClient().getChildren(lockPath);
    int index = children.indexOf(ZkKeyBuilder.parseIdFromPath(nodePath));

    if (children.size() == 0 || index == -1) {
      throw new SamzaException("Looks like we are no longer connected to Zk. Need to reconnect!");
    }

    if (index == 0) {
      hasLock.set(true);
      if (zkLockListener != null) {
        zkLockListener.onAcquiringLock();
      }
    } else {
      String predecessor = children.get(index - 1);
      if (!predecessor.equals(currentSubscription)) {
        if (currentSubscription != null) {
          zkUtils.unsubscribeDataChanges(lockPath + "/" + currentSubscription,
              previousProcessorChangeListener);
        }
        currentSubscription = predecessor;
        zkUtils.subscribeDataChanges(lockPath + "/" + currentSubscription,
            previousProcessorChangeListener);
      }
      /**
       * Verify that the predecessor still exists. This step is needed because the ZkClient subscribes for data changes
       * on the path, even if the path doesn't exist. Since we are using Ephemeral Sequential nodes, if the path doesn't
       * exist during subscription, it is not going to get created in the future.
       */
      boolean predecessorExists = zkUtils.exists(lockPath + "/" + currentSubscription);
      if (predecessorExists) {
        LOG.info("Predecessor still exists. Current subscription is valid. Continuing as non-lockholder.");
      } else {
        try {
          Thread.sleep(random.nextInt(1000));
        } catch (InterruptedException e) {
          Thread.interrupted();
        }
        LOG.info("Predecessor doesn't exist anymore. Trying to acquire lock again...");
        lock();
      }
    }
  }


  /**
   * Delete the node created to acquire the lock
   */
  @Override
  public void unlock() {
    if (nodePath != null) {
      Boolean status = false;
      while (!status && nodePath != null) {
        status = zkUtils.getZkClient().delete(nodePath);
      }
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

  class PreviousLockProcessorChangeListener implements IZkDataListener {
    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception {
      LOG.debug("Data change on path: " + dataPath + " Data: " + data);
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
      LOG.info("Data deleted on path " + dataPath + ". Predecessor went away. So, trying to become leader again...");
      lock();
    }
  }

}