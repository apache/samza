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

import com.google.common.collect.ImmutableList;
import java.util.Objects;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 *
 * Describes the lifecycle of a barrier.
 * <pre>
 *
 *                               When expected participants join
 *      Leader    ---&lt; NEW ---------------------------------------- &lt; DONE
 *                         |      barrier within barrierTimeOut.
 *                         |
 *                         |
 *                         |
 *                         |
 *                         |
 *                         |    When expected participants doesn't
 *                         | ----------------------------------------- &lt; TIMED_OUT
 *                              join barrier within barrierTimeOut.
 *
 * </pre>
 *
 *
 * The caller can listen to events associated with the barrier by registering a {@link ZkBarrierListener}.
 *
 * Zk Tree Reference:
 * /barrierRoot/
 *  |
 *  |- barrier_{version1}/
 *  |   |- barrier_state/
 *  |   |  ([NEW|DONE|TIMED_OUT])
 *  |   |- barrier_participants/
 *  |   |   |- {id1}
 *  |   |   |- {id2}
 *  |   |   |-  ...
 */
public class ZkBarrierForVersionUpgrade {
  private static final Logger LOG = LoggerFactory.getLogger(ZkBarrierForVersionUpgrade.class);
  private final ZkUtils zkUtils;
  private final BarrierKeyBuilder keyBuilder;
  private final Optional<ZkBarrierListener> barrierListenerOptional;
  private final ScheduleAfterDebounceTime debounceTimer;

  public enum State {
    NEW("NEW"), TIMED_OUT("TIMED_OUT"), DONE("DONE");

    private String str;

    State(String str) {
      this.str = str;
    }

    @Override
    public String toString() {
      return str;
    }
  }

  public ZkBarrierForVersionUpgrade(String barrierRoot, ZkUtils zkUtils, ZkBarrierListener barrierListener, ScheduleAfterDebounceTime debounceTimer) {
    if (zkUtils == null) {
      throw new RuntimeException("Cannot operate ZkBarrierForVersionUpgrade without ZkUtils.");
    }
    this.zkUtils = zkUtils;
    this.keyBuilder = new BarrierKeyBuilder(barrierRoot);
    this.barrierListenerOptional = Optional.ofNullable(barrierListener);
    this.debounceTimer = debounceTimer;
  }

  /**
   * Creates a shared barrier sub-tree in ZK
   *
   * @param version Version associated with the Barrier
   * @param participants List of expected participated for this barrier to complete
   */
  public void create(final String version, List<String> participants) {
    LOG.info(String.format("Creating barrier with version: %s, participants: %s.", version, participants));
    String barrierRoot = keyBuilder.getBarrierRoot();
    String barrierParticipantsPath = keyBuilder.getBarrierParticipantsPath(version);
    String barrierStatePath = keyBuilder.getBarrierStatePath(version);
    zkUtils.validatePaths(new String[]{
        barrierRoot,
        keyBuilder.getBarrierPath(version),
        barrierParticipantsPath,
        barrierStatePath});
    LOG.info("Marking the barrier state: {} as {}.", barrierStatePath, State.NEW);
    zkUtils.writeData(barrierStatePath, State.NEW);

    LOG.info("Subscribing child changes on the path: {} for barrier version: {}.", barrierParticipantsPath, version);
    zkUtils.subscribeChildChanges(barrierParticipantsPath, new ZkBarrierChangeHandler(version, participants, zkUtils));

    barrierListenerOptional.ifPresent(zkBarrierListener -> zkBarrierListener.onBarrierCreated(version));
  }

  /**
   * Joins a shared barrier by registering under the barrier sub-tree in ZK
   *
   * @param version Version associated with the Barrier
   * @param participantId Identifier of the participant
   */
  public void join(String version, String participantId) {
    LOG.info("Joining the barrier version: {} as participant: {}.", version, participantId);
    String barrierStatePath = keyBuilder.getBarrierStatePath(version);
    LOG.info("Subscribing data changes on the path: {} for barrier version: {}.", barrierStatePath, version);
    zkUtils.subscribeDataChanges(barrierStatePath, new ZkBarrierReachedHandler(barrierStatePath, version, zkUtils));

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
    String barrierStatePath = keyBuilder.getBarrierStatePath(version);
    State barrierState = zkUtils.getZkClient().readData(barrierStatePath);
    if (Objects.equals(barrierState, State.NEW)) {
      LOG.info(String.format("Expiring the barrier version: %s. Marking the barrier state: %s as %s.", version, barrierStatePath, State.TIMED_OUT));
      zkUtils.writeData(keyBuilder.getBarrierStatePath(version), State.TIMED_OUT);
    } else {
      LOG.debug(String.format("Barrier version: %s is at: %s state. Not marking barrier as %s.", version, barrierState, State.TIMED_OUT));
    }
  }

  /**
   * Listener for changes to the list of participants. It is meant to be subscribed only by the creator of the barrier
   * node. It checks to see when the barrier is ready to be marked as completed.
   */
  class ZkBarrierChangeHandler extends ZkUtils.GenIZkChildListener {
    private static final String ACTION_NAME = "ZkBarrierChangeHandler";

    private final String barrierVersion;
    private final List<String> expectedParticipantIds;

    public ZkBarrierChangeHandler(String barrierVersion, List<String> expectedParticipantIds, ZkUtils zkUtils) {
      super(zkUtils, ACTION_NAME);
      this.barrierVersion = barrierVersion;
      this.expectedParticipantIds = expectedParticipantIds;
    }

    @Override
    public void handleChildChange(String barrierParticipantPath, List<String> participantIds) {
      if (notAValidEvent()) {
        return;
      }
      if (participantIds == null) {
        LOG.info("Received notification with null participants for barrier: {}. Ignoring it.", barrierParticipantPath);
        return;
      }
      LOG.info(String.format("Current participants in barrier version: %s = %s.", barrierVersion, participantIds));
      LOG.info(String.format("Expected participants in barrier version: %s = %s.", barrierVersion, expectedParticipantIds));

      // check if all the expected participants are in
      if (participantIds.size() == expectedParticipantIds.size() && CollectionUtils.containsAll(participantIds, expectedParticipantIds)) {
        debounceTimer.scheduleAfterDebounceTime(ACTION_NAME, 0, () -> {
            String barrierStatePath = keyBuilder.getBarrierStatePath(barrierVersion);
            State barrierState = zkUtils.getZkClient().readData(barrierStatePath);
            if (Objects.equals(barrierState, State.NEW)) {
              LOG.info(String.format("Expected participants has joined the barrier version: %s. Marking the barrier state: %s as %s.", barrierVersion, barrierStatePath, State.DONE));
              zkUtils.writeData(barrierStatePath, State.DONE); // this will trigger notifications
            } else {
              LOG.debug(String.format("Barrier version: %s is at: %s state. Not marking barrier as %s.", barrierVersion, barrierState, State.DONE));
            }
            LOG.info("Unsubscribing child changes on the path: {} for barrier version: {}.", barrierParticipantPath, barrierVersion);
            zkUtils.unsubscribeChildChanges(barrierParticipantPath, this);
          });
      }
    }
  }

  /**
   * Listener for changes to the Barrier state. It is subscribed by all participants of the barrier, including the
   * participant that creates the barrier.
   * Barrier state values are either DONE or TIMED_OUT. It only registers to receive on valid state change notification.
   * Once a valid state change notification is received, it will un-subscribe from further notifications.
   */
  class ZkBarrierReachedHandler extends ZkUtils.GenIZkDataListener {
    private final String barrierStatePath;
    private final String barrierVersion;

    public ZkBarrierReachedHandler(String barrierStatePath, String version, ZkUtils zkUtils) {
      super(zkUtils, "ZkBarrierReachedHandler");
      this.barrierStatePath = barrierStatePath;
      this.barrierVersion = version;
    }

    @Override
    public void handleDataChange(String dataPath, Object data) {
      LOG.info(String.format("Received barrierState change notification for barrier version: %s from zkNode: %s with data: %s.", barrierVersion, dataPath, data));
      if (notAValidEvent())
        return;

      State barrierState = (State) data;
      List<State> expectedBarrierStates = ImmutableList.of(State.DONE, State.TIMED_OUT);

      if (barrierState != null && expectedBarrierStates.contains(barrierState)) {
        zkUtils.unsubscribeDataChanges(barrierStatePath, this);
        barrierListenerOptional.ifPresent(zkBarrierListener -> zkBarrierListener.onBarrierStateChanged(barrierVersion, (State) data));
      } else {
        LOG.debug("Barrier version: {} is at state: {}. Ignoring the barrierState change notification.", barrierVersion, barrierState);
      }
    }

    @Override
    public void handleDataDeleted(String dataPath) {
      LOG.warn("barrier done node got deleted at " + dataPath);
      if (notAValidEvent())
        return;
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

  public static int getVersion(String barrierPath) {
    return Integer.valueOf(barrierPath.substring(barrierPath.lastIndexOf('_') + 1));
  }
}
