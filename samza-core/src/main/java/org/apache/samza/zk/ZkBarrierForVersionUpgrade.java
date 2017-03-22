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

import java.util.Arrays;
import java.util.List;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.exception.ZkBadVersionException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.samza.SamzaException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class creates a barrier for version upgrade.
 * Barrier is started by the participant responsible for the upgrade. (start())
 * Each participant will mark its readiness and register for a notification when the barrier is reached. (waitFor())
 * If a timer (started in start()) goes off before the barrier is reached, all the participants will unsubscribe
 * from the notification and the barrier becomes invalid.
 */
public class ZkBarrierForVersionUpgrade implements BarrierForVersionUpgrade {
  private final ZkUtils zkUtils;
  private final ZkKeyBuilder keyBuilder;
  private final static String BARRIER_DONE = "done";
  private final static String BARRIER_TIMED_OUT = "TIMED_OUT";
  private final static long BARRIER_TIMED_OUT_MS = 60 * 1000;
  private final static Logger LOG = LoggerFactory.getLogger(ZkBarrierForVersionUpgrade.class);

  private final ScheduleAfterDebounceTime debounceTimer;

  private final String barrierPrefix;
  private final String barrierPath;
  private final String barrierDonePath;
  private final String barrierProcessors;
  private final String version;
  private final List<String> processorsNames;
  private static final String VERSION_UPGRADE_TIMEOUT_TIMER = "VersionUpgradeTimeout";

  public ZkBarrierForVersionUpgrade(ZkUtils zkUtils, ScheduleAfterDebounceTime debounceTimer, String version, List<String> processorsNames) {
    this.zkUtils = zkUtils;
    keyBuilder = zkUtils.getKeyBuilder();

    barrierPrefix = keyBuilder.getJobModelVersionBarrierPrefix();
    this.debounceTimer = debounceTimer;

    barrierPath = String.format("%s/barrier_%s", barrierPrefix, version);
    barrierDonePath = String.format("%s/barrier_done", barrierPath);
    barrierProcessors = String.format("%s/barrier_processors", barrierPath);

    this.version = version;
    this.processorsNames = processorsNames;

    zkUtils.makeSurePersistentPathsExists(new String[]{barrierPrefix, barrierPath, barrierProcessors, barrierDonePath});
  }

  /**
   * set the barrier for the timer. If the timer is not achieved by the timeout - it will fail
   * @param version for which the barrier is created
   * @param timeout - time in ms to wait
   */
  private void setTimer(final String version, final long timeout, final Stat currentStatOfBarrierDone) {
    debounceTimer.scheduleAfterDebounceTime(VERSION_UPGRADE_TIMEOUT_TIMER, timeout, ()->timerOff(version, currentStatOfBarrierDone));
  }

  protected long getBarrierTimeOutMs() {
    return BARRIER_TIMED_OUT_MS;
  }

  private void timerOff(final String version, final Stat currentStatOfBarrierDone) {
    try {
      // write a new value "TIMED_OUT", if the value was changed since previous value, make sure it was changed to "DONE"
      zkUtils.getZkClient().writeData(barrierDonePath, BARRIER_TIMED_OUT, currentStatOfBarrierDone.getVersion());
    } catch (ZkBadVersionException e) {
      // failed to write, make sure the value is "DONE"
      LOG.warn("Barrier timeout write failed");
      String done = zkUtils.getZkClient().<String>readData(barrierDonePath);
      if (!done.equals(BARRIER_DONE)) {
        throw new SamzaException("Failed to write to the barrier_done, version=" + version, e);
      }
    }
  }

  @Override
  public void start() {

    // subscribe for processor's list changes
    LOG.info("Subscribing for child changes at " + barrierProcessors);
    zkUtils.getZkClient().subscribeChildChanges(barrierProcessors, new ZkBarrierChangeHandler(version, processorsNames));

    // create a timer for time-out
    Stat currentStatOfBarrierDone = new Stat();
    zkUtils.getZkClient().readData(barrierDonePath, currentStatOfBarrierDone);
    setTimer(version, getBarrierTimeOutMs(), currentStatOfBarrierDone);
  }

  @Override
  public void waitForBarrier(String processorsName, Runnable callback) {
    final String barrierProcessorThis = String.format("%s/%s", barrierProcessors, processorsName);

    // update the barrier for this processor
    LOG.info("Creating a child for barrier at " + barrierProcessorThis);
    zkUtils.getZkClient().createPersistent(barrierProcessorThis);

    // now subscribe for the barrier
    zkUtils.getZkClient().subscribeDataChanges(barrierDonePath, new ZkBarrierReachedHandler(barrierDonePath, debounceTimer, callback));
  }

  /**
   * listener for the subscription.
   */
  class ZkBarrierChangeHandler implements IZkChildListener {
    private final String version;
    private final List<String> names;

    public ZkBarrierChangeHandler(String version, List<String> names) {
      this.version = version;
      this.names = names;
    }

    @Override
    public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {

      if (currentChildren == null) {
        LOG.info("Got handleChildChange with null currentChildren");
        return;
      }
      LOG.info("list of children in the barrier = " + parentPath + ":" + Arrays.toString(currentChildren.toArray()));
      LOG.info("list of children to compare against = " + parentPath + ":" + Arrays.toString(names.toArray()));

      // check if all the names are in
      if (CollectionUtils.containsAll(names, currentChildren)) {
        LOG.info("ALl nodes reached the barrier");
        LOG.info("Writing BARRIER DONE to " + barrierDonePath);
        zkUtils.getZkClient().writeData(barrierDonePath, BARRIER_DONE);
      }
    }
  }

  class ZkBarrierReachedHandler implements IZkDataListener {
    private final ScheduleAfterDebounceTime debounceTimer;
    private final String barrierPathDone;
    private final Runnable callback;

    public ZkBarrierReachedHandler(String barrierPathDone, ScheduleAfterDebounceTime debounceTimer, Runnable callback) {
      this.barrierPathDone = barrierPathDone;
      this.callback = callback;
      this.debounceTimer = debounceTimer;
    }

    @Override
    public void handleDataChange(String dataPath, Object data)
        throws Exception {
      String done = (String) data;
      LOG.info("got notification about barrier path=" + barrierPathDone + "; done=" + done);
      if (done.equals(BARRIER_DONE)) {
        zkUtils.unsubscribeDataChanges(barrierPathDone, this);
        debounceTimer.scheduleAfterDebounceTime(ScheduleAfterDebounceTime.JOB_MODEL_VERSION_CHANGE, 0, callback);
      } else if (done.equals(BARRIER_TIMED_OUT)) {
        // timed out
        LOG.warn("Barrier for " + dataPath + " timed out");
        LOG.info("Barrier for " + dataPath + " timed out");
        zkUtils.unsubscribeDataChanges(barrierPathDone, this);
      }
      // we do not need to resubscribe because, ZkClient library does it for us.

    }

    @Override
    public void handleDataDeleted(String dataPath)
        throws Exception {
      LOG.warn("barrier done got deleted at " + dataPath);
    }
  }
}
