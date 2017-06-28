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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * ZkBarrierForVersionUpgrade is an implementation of distributed barrier, which guarantees that the expected barrier
 * size and barrier participants match before marking the barrier as complete.
 * It also allows the caller to expire the barrier.
 *
 * This implementation is specifically tailored towards barrier support during jobmodel version upgrades. The participant
 * responsible for the version upgrade starts the barrier by invoking {@link #create(String, List)}.
 * Each participant in the list, then, joins the new barrier. When all listed participants {@link #join(String, String)}
 * the barrier, the creator marks the barrier as {@link org.apache.samza.zk.ZkBarrierForVersionUpgrade.State#DONE}
 * which signals the end of barrier.
 * The creator of the barrier can expire the barrier by invoking {@link #expire(String)}. This will mark the barrier
 * with value {@link org.apache.samza.zk.ZkBarrierForVersionUpgrade.State#TIMED_OUT} and indicates to everyone that it
 * is no longer valid.
 *
 * The caller can listen to events associated with the barrier by registering a {@link ZkBarrierListener}.
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
public class ZkBarrierForVersionUpgrade {
  private final static Logger LOG = LoggerFactory.getLogger(ZkBarrierForVersionUpgrade.class);
  private final ZkUtils zkUtils;
  private final BarrierKeyBuilder keyBuilder;
  private final Optional<ZkBarrierListener> barrierListenerOptional;

  public enum State {
    TIMED_OUT, DONE
  }


  public ZkBarrierForVersionUpgrade(String barrierRoot, ZkUtils zkUtils, ZkBarrierListener barrierListener) {
    if (zkUtils == null) {
      throw new RuntimeException("Cannot operate ZkBarrierForVersionUpgrade without ZkUtils.");
    }
    this.zkUtils = zkUtils;
    this.keyBuilder = new BarrierKeyBuilder(barrierRoot);
    this.barrierListenerOptional = Optional.ofNullable(barrierListener);
  }

  /**
   * Creates a shared barrier sub-tree in ZK
   *
   * @param version Version associated with the Barrier
   * @param participants List of expected participated for this barrier to complete
   */
  public void create(final String version, List<String> participants) {
    String barrierRoot = keyBuilder.getBarrierRoot();
    String barrierParticipantsPath = keyBuilder.getBarrierParticipantsPath(version);
    zkUtils.makeSurePersistentPathsExists(new String[]{
        barrierRoot,
        keyBuilder.getBarrierPath(version),
        barrierParticipantsPath,
        keyBuilder.getBarrierStatePath(version)});

    // subscribe for participant's list changes
    LOG.info("Subscribing for child changes at " + barrierParticipantsPath);
    zkUtils.subscribeChildChanges(barrierParticipantsPath, new ZkBarrierChangeHandler(version, participants));

    barrierListenerOptional.ifPresent(zkBarrierListener -> zkBarrierListener.onBarrierCreated(version));
  }

  /**
   * Joins a shared barrier by registering under the barrier sub-tree in ZK
   *
   * @param version Version associated with the Barrier
   * @param participantId Identifier of the participant
   */
  public void join(String version, String participantId) {
    String barrierDonePath = keyBuilder.getBarrierStatePath(version);
    zkUtils.subscribeDataChanges(barrierDonePath, new ZkBarrierReachedHandler(barrierDonePath, version));

    // TODO: Handle ZkNodeExistsException - SAMZA-1304
    zkUtils.getZkClient().createPersistent(
        String.format("%s/%s", keyBuilder.getBarrierParticipantsPath(version), participantId));
  }

  /**
   * Expires the barrier version by marking it as TIMED_OUT
   *
   * @param version Version associated with the Barrier
   */
  public void expire(String version) {
    zkUtils.writeData(
        keyBuilder.getBarrierStatePath(version),
        State.TIMED_OUT);

  }
  /**
   * Listener for changes to the list of participants. It is meant to be subscribed only by the creator of the barrier
   * node. It checks to see when the barrier is ready to be marked as completed.
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
      LOG.info("list of children in the barrier = " + parentPath + ":" + Arrays.toString(currentChildren.toArray()));
      LOG.info("list of children to compare against = " + parentPath + ":" + Arrays.toString(names.toArray()));

      // check if all the expected participants are in
      if (currentChildren.size() == names.size() && CollectionUtils.containsAll(currentChildren, names)) {
        String barrierDonePath = keyBuilder.getBarrierStatePath(barrierVersion);
        LOG.info("Writing BARRIER DONE to " + barrierDonePath);
        zkUtils.writeData(barrierDonePath, State.DONE); // this will trigger notifications
        zkUtils.unsubscribeChildChanges(barrierDonePath, this);
      }
    }
  }

  /**
   * Listener for changes to the Barrier state. It is subscribed by all participants of the barrier, including the
   * participant that creates the barrier.
   * Barrier state values are either DONE or TIMED_OUT. It only registers to receive on valid state change notification.
   * Once a valid state change notification is received, it will un-subscribe from further notifications.
   */
  class ZkBarrierReachedHandler implements IZkDataListener {
    private final String barrierStatePath;
    private final String barrierVersion;

    public ZkBarrierReachedHandler(String barrierStatePath, String version) {
      this.barrierStatePath = barrierStatePath;
      this.barrierVersion = version;
    }

    @Override
    public void handleDataChange(String dataPath, Object data) {
      LOG.info("got notification about barrier " + barrierStatePath + "; done=" + data);
      zkUtils.unsubscribeDataChanges(barrierStatePath, this);
      barrierListenerOptional.ifPresent(
          zkBarrierListener -> zkBarrierListener.onBarrierStateChanged(barrierVersion, (State) data));
    }

    @Override
    public void handleDataDeleted(String dataPath)
        throws Exception {
      LOG.warn("barrier done got deleted at " + dataPath);
    }
  }

  class BarrierKeyBuilder {
    private static final String BARRIER_PARTICIPANTS = "/barrier_participants";
    private static final String BARRIER_STATE = "/barrier_state";
    private final String barrierRoot;
    BarrierKeyBuilder(String barrierRoot) {
      if (barrierRoot == null || barrierRoot.trim().isEmpty() || !barrierRoot.trim().startsWith("/")) {
        throw new IllegalArgumentException("Barrier root path cannot be null or empty and the path has to start with '/'");
      }
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
