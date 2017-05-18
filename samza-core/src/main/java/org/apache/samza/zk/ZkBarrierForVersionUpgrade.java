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

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.samza.coordinator.BarrierForVersionUpgrade;
import org.apache.samza.coordinator.BarrierForVersionUpgradeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;


/**
 * This class creates a barrier for version upgrade.
 * Barrier is started by the participant responsible for the upgrade. (start())
 * Each participant will mark its readiness and register for a notification when the barrier is reached. (waitFor())
 * If a timer (started in start()) goes off before the barrier is reached, all the participants will unsubscribe
 * from the notification and the barrier becomes invalid.
 *
 * Zk Tree Reference:
 * /barrierRoot/
 *  |
 *  |- barrier_{version1}/
 *  |   |- barrier_state/
 *  |   |  ([DONE|TIMED_OUT])
 *  |   |- barrier_participants/
 *  |   |   |- {id1}
 *  |   |   |- {id2}
 *  |   |   |-  ...
 */
public class ZkBarrierForVersionUpgrade implements BarrierForVersionUpgrade {
  private final static Logger LOG = LoggerFactory.getLogger(ZkBarrierForVersionUpgrade.class);
  private static final String BARRIER_PARTICIPANTS = "/barrier_participants";
  private static final String BARRIER_STATE = "/barrier_state";

  private final ZkUtils zkUtils;
  private final BarrierKeyBuilder keyBuilder;

  private BarrierForVersionUpgradeListener barrierListener;

  public ZkBarrierForVersionUpgrade(String barrierRoot, ZkUtils zkUtils, BarrierForVersionUpgradeListener barrierListener) {
    if (zkUtils == null) {
      throw new RuntimeException("Cannot operate ZkBarrierForVersionUpgrade without ZkUtils.");
    }
    this.zkUtils = zkUtils;
    this.keyBuilder = new BarrierKeyBuilder(barrierRoot);
    this.barrierListener = barrierListener;
  }

  @Override
  public void start(final String version, List<String> participants) {
    String barrierRoot = keyBuilder.getBarrierRoot();
    zkUtils.makeSurePersistentPathsExists(new String[]{
        barrierRoot,
        keyBuilder.getBarrierPath(version),
        keyBuilder.getBarrierParticipantsPath(version),
        keyBuilder.getBarrierStatePath(version)});

    // subscribe for participant's list changes
    String barrierParticipantsPath = keyBuilder.getBarrierParticipantsPath(version);
    LOG.info("Subscribing for child changes at " + barrierParticipantsPath);
    zkUtils.getZkClient().subscribeChildChanges(barrierParticipantsPath, new ZkBarrierChangeHandler(version, participants));

    if (barrierListener != null) {
      barrierListener.onBarrierCreated(version);
    }
  }

  @Override
  public void joinBarrier(String version, String participantId) {
    String barrierDonePath = keyBuilder.getBarrierStatePath(version);
    zkUtils.getZkClient().subscribeDataChanges(barrierDonePath, new ZkBarrierReachedHandler(barrierDonePath, version));

    zkUtils.getZkClient().createPersistent(
        String.format("%s/%s", keyBuilder.getBarrierParticipantsPath(version), participantId));
  }

  @Override
  public void setBarrierForVersionUpgrade(BarrierForVersionUpgradeListener listener) {
    this.barrierListener = listener;
  }

  public void expireBarrier(String version) {
    zkUtils.getZkClient().writeData(
        keyBuilder.getBarrierStatePath(version),
        BarrierForVersionUpgrade.State.TIMED_OUT);

  }
  /**
   * Listener for the subscription for the list of participants.
   * This method will mark the barrier as DONE when all the participants have joined.
   */
  class ZkBarrierChangeHandler implements IZkChildListener {
    private final String barrierVersion;
    private final List<String> names;

    public ZkBarrierChangeHandler(String barrierVersion, List<String> names) {
      this.barrierVersion = barrierVersion;
      this.names = names;
    }

    @Override
    public void handleChildChange(String parentPath, List<String> currentChildren) {
      if (currentChildren == null) {
        LOG.info("Got ZkBarrierChangeHandler handleChildChange with null currentChildren");
        return;
      }
      LOG.debug("list of children in the barrier = " + parentPath + ":" + Arrays.toString(currentChildren.toArray()));
      LOG.debug("list of children to compare against = " + parentPath + ":" + Arrays.toString(names.toArray()));

      // check if all the expected participants are in
      if (CollectionUtils.containsAll(currentChildren, names)) {
        String barrierDonePath = keyBuilder.getBarrierStatePath(barrierVersion);
        LOG.info("Writing BARRIER DONE to " + barrierDonePath);
        zkUtils.getZkClient().writeData(barrierDonePath, State.DONE); // this will trigger notifications
        zkUtils.getZkClient().unsubscribeChildChanges(barrierDonePath, this);
      }
    }
  }

  class ZkBarrierReachedHandler implements IZkDataListener {
    private final String barrierPathDone;
    private final String barrierVersion;

    public ZkBarrierReachedHandler(String barrierPathDone, String version) {
      this.barrierPathDone = barrierPathDone;
      this.barrierVersion = version;
    }

    @Override
    public void handleDataChange(String dataPath, Object data) {
      LOG.info("got notification about barrier " + barrierPathDone + "; done=" + data);
      zkUtils.unsubscribeDataChanges(barrierPathDone, this);
      if (barrierListener != null) {
        barrierListener.onBarrierStateChanged(barrierVersion, (State) data);
      }
    }

    @Override
    public void handleDataDeleted(String dataPath)
        throws Exception {
      LOG.warn("barrier done got deleted at " + dataPath);
    }
  }

  class BarrierKeyBuilder {
    private final String barrierRoot;
    BarrierKeyBuilder(String barrierRoot) {
      this.barrierRoot = barrierRoot;
    }

    String getBarrierRoot() {
      return barrierRoot;
    }

    String getBarrierPath(String version) {
      return String.format("%s/barrier_%s", barrierRoot, version);
    }

    String getBarrierParticipantsPath(String version) {
      return getBarrierPath(version) + BARRIER_PARTICIPANTS;
    }

    String getBarrierStatePath(String version) {
      return getBarrierPath(version) + BARRIER_STATE;
    }
  }

}
