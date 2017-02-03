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

import org.I0Itec.zkclient.IZkDataListener;
import org.apache.samza.SamzaException;
import org.apache.samza.coordinator.leaderelection.LeaderElector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class ZkLeaderElector implements LeaderElector {
  public static final Logger LOGGER = LoggerFactory.getLogger(ZkLeaderElector.class);
  private final ZkUtils zkUtils;
  private final String processorIdStr;
  private final ZkKeyBuilder keyBuilder;
  private final String hostName;

  private String leaderId = null;
  private final ZkLeaderListener zkLeaderListener = new ZkLeaderListener();
  private String currentSubscription = null;
  private final Random random = new Random();

  public ZkLeaderElector(String processorIdStr, ZkUtils zkUtils) {
    this.processorIdStr = processorIdStr;
    this.zkUtils = zkUtils;
    this.keyBuilder = this.zkUtils.getKeyBuilder();
    this.hostName = getHostName();
  }

  private String getHostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOGGER.error("Failed to fetch hostname of the processor", e);
      throw new SamzaException(e);
    }
  }
  @Override
  public boolean tryBecomeLeader() {
    String currentPath = zkUtils.getEphemeralPath();

    if (currentPath == null || currentPath.isEmpty()) {
      zkUtils.registerProcessorAndGetId(hostName);
      currentPath = zkUtils.getEphemeralPath();
    }

    List<String> children = zkUtils.getActiveProcessors();
    int index = children.indexOf(ZkKeyBuilder.parseIdFromPath(currentPath));

    if (index == -1) {
      // Retry register here??
      throw new SamzaException("Looks like we are no longer connected to Zk. Need to reconnect??");
    }

    if (index == 0) {
      LOGGER.info("pid=" + processorIdStr + " Eligible to be the leader!");
      leaderId = ZkKeyBuilder.parseIdFromPath(currentPath);
      return true;
    }

    LOGGER.info("pid=" + processorIdStr + ";index=" + index + ";children=" + Arrays.toString(children.toArray()) + " Not eligible to be a leader yet!");
    leaderId = ZkKeyBuilder.parseIdFromPath(children.get(0));
    String prevCandidate = children.get(index - 1);
    if (!prevCandidate.equals(currentSubscription)) {
      if (currentSubscription != null) {
        zkUtils.unsubscribeDataChanges(keyBuilder.getProcessorsPath() + "/" + currentSubscription, zkLeaderListener);
      }
      currentSubscription = prevCandidate;
      LOGGER.info("pid=" + processorIdStr + "Subscribing to " + prevCandidate);
      zkUtils.subscribeDataChanges(keyBuilder.getProcessorsPath() + "/" + currentSubscription, zkLeaderListener);
    }

    // Double check that the previous candidate still exists
    boolean prevCandidateExists = zkUtils.exists(keyBuilder.getProcessorsPath() + "/" + currentSubscription);
    if (prevCandidateExists) {
      LOGGER.info("pid=" + processorIdStr + "Previous candidate still exists. Continuing as non-leader");
    } else {
      // TODO - what actually happens here..
      try {
        Thread.sleep(random.nextInt(1000));
      } catch (InterruptedException e) {
        Thread.interrupted();
      }
      LOGGER.info("pid=" + processorIdStr + "Previous candidate doesn't exist anymore. Trying to become leader again...");
      return tryBecomeLeader();
    }
    return false;
  }

  @Override
  public void resignLeadership() {

  }

  @Override
  public boolean amILeader() {
    return zkUtils.getEphemeralPath() != null
        && leaderId != null
        && leaderId.equals(ZkKeyBuilder.parseIdFromPath(zkUtils.getEphemeralPath()));
  }

  // Only by non-leaders
  class ZkLeaderListener implements IZkDataListener {

    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception {
      LOGGER.debug("ZkLeaderListener::handleDataChange on path " + dataPath + " Data: " + data);
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
      LOGGER.info("ZkLeaderListener::handleDataDeleted on path " + dataPath);
      tryBecomeLeader();
    }
  }
}
