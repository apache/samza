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
import org.apache.samza.SamzaException;
import org.apache.samza.coordinator.DistributedReadWriteLock;
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

  public ZkDistributedReadWriteLock(String participantId, ZkUtils zkUtils) {
    if (zkUtils == null) {
      throw new RuntimeException("Cannot operate ZkDistributedReadWriteLock without ZkUtils.");
    }
    this.zkUtils = zkUtils;
    this.participantId = participantId;
    this.keyBuilder = zkUtils.getKeyBuilder();
    lockPath = String.format("%s/readWriteLock", keyBuilder.getRootPath());
    particpantsPath = String.format("%s/%s", lockPath, PARTICIPANTS_PATH);
    processorsPath = String.format("%s/%s", lockPath, PROCESSORS_PATH);
    zkUtils.validatePaths(new String[] {lockPath, particpantsPath, processorsPath});
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
      List<String> participants = zkUtils.getZkClient().getChildren(particpantsPath);
      int index = participants.indexOf(ZkKeyBuilder.parseIdFromPath(activeParticipantPath));

      if (participants.size() == 0 || index == -1) {
        throw new SamzaException("Looks like we are no longer connected to Zk. Need to reconnect!");
      }
      // Acquires lock when the node has the lowest sequence number and returns.
      if (index == 0) {
        LOG.info("Acquired access to list of active processors for participant id: {}", participantId);
        activeProcessorPath = zkUtils.getZkClient().createEphemeralSequential(processorsPath + "/", participantId);
        LOG.info("created ephemeral sequential node for active processor at {}", activeProcessorPath);
        List<String> processors = zkUtils.getZkClient().getChildren(processorsPath);
        if (processors.size() == 0) {
          throw new SamzaException("Looks like we are no longer connected to Zk. Need to reconnect!");
        }
        if(processors.size() == 1) {
          return AccessType.WRITE;
        } else {
          return AccessType.READ;
        }
      } else {
        try {
          Thread.sleep(random.nextInt(1000));
        } catch (InterruptedException e) {
          Thread.interrupted();
        }
        LOG.info("Trying to acquire lock again...");
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
  }

  /**
   * Cleans state by removing the ephemeral node under "processors"
   */
  @Override
  public void cleanState() {
    if(activeProcessorPath != null && zkUtils.exists(activeProcessorPath)) {
      zkUtils.getZkClient().delete(activeProcessorPath);
      activeProcessorPath = null;
      LOG.info("Ephemeral active processor node for read write lock deleted.");
    } else {
      LOG.warn("Ephemeral active processor node for read write lock you want to delete doesn't exist");
    }
  }
}