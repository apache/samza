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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.I0Itec.zkclient.IZkStateListener;
import org.apache.samza.SamzaException;
import org.apache.samza.coordinator.DistributedReadWriteLock;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Distributed lock primitive for Zookeeper.
 */
public class ZkDistributedReadWriteLock implements DistributedReadWriteLock {

  public static final Logger LOG = LoggerFactory.getLogger(ZkDistributedReadWriteLock.class);
  private static final String PARTICIPANTS_PATH = "participants";
  private static final String PROCESSORS_PATH = "processors";
  private final ZkUtils zkUtils;
  private final String lockPath;
  private final String particpantsPath;
  private final String processorsPath;
  private final String participantId;
  private final ZkKeyBuilder keyBuilder;
  private final Random random = new Random();
  private String activeParticipantPath = null;
  private String activeProcessorPath = null;
  private final Object mutex;
  private boolean isInCriticalSection = false;
  private boolean isStateLost = false;

  public ZkDistributedReadWriteLock(String participantId, ZkUtils zkUtils, String lockId) {
    if (zkUtils == null) {
      throw new RuntimeException("Cannot operate ZkDistributedReadWriteLock without ZkUtils.");
    }
    this.zkUtils = zkUtils;
    this.participantId = participantId;
    this.keyBuilder = zkUtils.getKeyBuilder();
    lockPath = String.format("%s/readWriteLock-%s", keyBuilder.getRootPath(), lockId);
    particpantsPath = String.format("%s/%s", lockPath, PARTICIPANTS_PATH);
    processorsPath = String.format("%s/%s", lockPath, PROCESSORS_PATH);
    zkUtils.validatePaths(new String[] {lockPath, particpantsPath, processorsPath});
    mutex = new Object();
    zkUtils.getZkClient().subscribeChildChanges(particpantsPath, new ParticipantChangeHandler(zkUtils));
    zkUtils.getZkClient().subscribeStateChanges(new ZkSessionStateChangedListener());
  }

  /**
   * Tries to acquire a lock in order to generate run.id. On failure to acquire lock, it keeps trying until the lock times out.
   * Creates a sequential ephemeral node under "participants" to acquire the lock.
   * If the path of this node has the lowest sequence number,
   * it creates a sequential ephemeral node under "processors" and checks the number of nodes under "processors"
   * if there is only one node under "processors", a WRITE access lock is acquired
   * else a READ access lock is acquired
   * @param timeout Duration of lock acquiring timeout.
   * @param unit Unit of the timeout defined above.
   * @return AccessType.READ/WRITE if lock is acquired successfully, AccessType.NONE if it times out.
   */
  @Override
  public AccessType lock(long timeout, TimeUnit unit)
      throws TimeoutException {

    activeParticipantPath = zkUtils.getZkClient().createEphemeralSequential(particpantsPath + "/", participantId);

    //Start timer for timeout
    long startTime = System.currentTimeMillis();
    long lockTimeout = TimeUnit.MILLISECONDS.convert(timeout, unit);

    while ((System.currentTimeMillis() - startTime) < lockTimeout) {
      synchronized (mutex) {
        AccessType accessType = checkAndAcquireLock();
        if (accessType != AccessType.NONE) {
          isInCriticalSection = true;
          if (isStateLost) {
            throw new SamzaException("Lock's state lost due to connection expiry");
          }
          return accessType;
        } else {
          if (isStateLost) {
            throw new SamzaException("Lock's state lost due to connection expiry");
          }
          try {
            mutex.wait(lockTimeout);
          } catch (InterruptedException e) {
            throw new SamzaException("Failed to lock", e);
          }
        }
      }
    }
    throw new TimeoutException("could not acquire lock for " + timeout + " " + unit.toString());
  }

  /**
   * Unlocks, by deleting the ephemeral sequential node under "participants" created to acquire the lock
   */
  @Override
  public void unlock() {
    if (activeParticipantPath != null && zkUtils.exists(activeParticipantPath)) {
      zkUtils.getZkClient().delete(activeParticipantPath);
      activeParticipantPath = null;
      LOG.info("Ephemeral active participant node for read write lock deleted. Unlocked!");
    } else {
      LOG.warn("Ephemeral active participant node for read write lock you want to delete doesn't exist");
    }
    isInCriticalSection = false;
  }

  /**
   * Cleans state by removing the ephemeral node under "processors"
   */
  @Override
  public void cleanState() {
    if (activeProcessorPath != null && zkUtils.exists(activeProcessorPath)) {
      zkUtils.getZkClient().delete(activeProcessorPath);
      activeProcessorPath = null;
      LOG.info("Ephemeral active processor node for read write lock deleted.");
    } else {
      LOG.warn("Ephemeral active processor node for read write lock you want to delete doesn't exist");
    }
  }

  private AccessType checkAndAcquireLock() {
    List<String> participants = zkUtils.getZkClient().getChildren(particpantsPath);
    int index = participants.indexOf(ZkKeyBuilder.parseIdFromPath(activeParticipantPath));

    if (participants.size() == 0 || index == -1) {
      throw new SamzaException("Looks like we are no longer connected to Zk. Need to reconnect!");
    }
    // Acquires lock when the node has the lowest sequence number and returns.
    if (index == 0) {
      LOG.info("Acquired access to read list of active processors for participant id: {}", participantId);
      createActiveProcessorNode();
      List<String> processors = zkUtils.getZkClient().getChildren(processorsPath);
      if (processors.size() == 0) {
        throw new SamzaException("Looks like we are no longer connected to Zk. Need to reconnect!");
      }
      if (processors.size() == 1) {
        return AccessType.WRITE;
      } else {
        return AccessType.READ;
      }
    } else {
      return AccessType.NONE;
    }
  }

  private void createActiveProcessorNode() {
    if (activeProcessorPath == null || !zkUtils.exists(activeProcessorPath)) {
      activeProcessorPath = zkUtils.getZkClient().createEphemeralSequential(processorsPath + "/", participantId);
    }
  }

  class ParticipantChangeHandler extends ZkUtils.GenerationAwareZkChildListener {

    public ParticipantChangeHandler(ZkUtils zkUtils) {
      super(zkUtils, "ParticipantChangeHandler");
    }

    /**
     * Called when the children of the given path changed.
     *
     * @param parentPath      The parent path
     * @param currentChildren The children or null if the root node (parent path) was deleted.
     * @throws Exception
     */
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

  class ZkSessionStateChangedListener implements IZkStateListener {
    @Override
    public void handleStateChanged(Watcher.Event.KeeperState state) {
      switch (state) {
        case Expired:
          // if the session has expired it means that all the ephemeral nodes are gone.
          LOG.warn("Got " + state.toString() + " event for processor=" + participantId + ".");
          isStateLost = true;
        default:
          // received Disconnected, AuthFailed, SyncConnected, ConnectedReadOnly, and SaslAuthenticated. NoOp
          LOG.info("Got ZK event " + state.toString() + " for processor=" + participantId + ". Continue");
      }
    }

    @Override
    public void handleNewSession() {
      if (isInCriticalSection) {
        isStateLost = true;
      } else {
        // TODO: Manasa: should I be the first participant to be able to create processor node?
        createActiveProcessorNode();
      }
    }

    @Override
    public void handleSessionEstablishmentError(Throwable error) {
      // this means we cannot connect to zookeeper to establish a session
      LOG.info("handleSessionEstablishmentError received for processor=" + participantId, error);
      isStateLost = true;
    }
  }
}