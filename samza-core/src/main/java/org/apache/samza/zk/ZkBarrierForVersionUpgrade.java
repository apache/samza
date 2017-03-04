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
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkBarrierForVersionUpgrade implements BarrierForVersionUpgrade {
  private final ZkUtils zkUtils;
  private final ZkKeyBuilder keyBuilder;
  private final static String BARRIER_DONE = "done";
  private final static String BARRIER_TIMED_OUT = "TIMED_OUT";
  private final static long BARRIER_TIMED_OUT_MS = 60*1000;
  private final static Logger LOG = LoggerFactory.getLogger(ZkBarrierForVersionUpgrade.class);

  private final ScheduleAfterDebounceTime debounceTimer;

  private final String barrierPrefix;

  public ZkBarrierForVersionUpgrade(ZkUtils zkUtils, ScheduleAfterDebounceTime debounceTimer) {
    this.zkUtils = zkUtils;
    keyBuilder = zkUtils.getKeyBuilder();

    barrierPrefix = keyBuilder.getJobModelVersionBarrierPrefix();
    this.debounceTimer = debounceTimer;
  }

  /**
   * set the barrier for the timer. If the timer is not achieved by the timeout - it will fail
   * @param version for which the barrier is created
   * @param timeout - time in ms to wait
   */
  public void setTimer(String version, long timeout) {
    debounceTimer.scheduleAfterDebounceTime("VersionUpgradeTimeout", timeout, ()->timerOff(version));
  }

  private void timerOff(String version) {
    // check if barrier has finished
    final String barrierPath = String.format("%s/barrier_%s", barrierPrefix, version);
    final String barrierDonePath = String.format("%s/barrier_done", barrierPath);
    Stat stat = new Stat();
    String done = zkUtils.getZkClient().<String>readData(barrierDonePath, stat);
    if(done != null && done.equals(BARRIER_DONE))
      return; //nothing to do

    while(true) {
      try {
        // write a new value if no one else did, if the value was changed since previous reading - retry
        zkUtils.getZkClient().writeData(barrierDonePath, "TIMED_OUT", stat.getVersion());
        return;
      } catch (Exception e) {
        // failed to write, try read/write again
        LOG.info("Barrier timeout write failed");
        done = zkUtils.getZkClient().<String>readData(barrierDonePath, stat);
        if(done.equals(BARRIER_DONE))
          return; //nothing to do
      }
    }
  }

  @Override
  public void start(String version, List<String> processorsNames) {
    final String barrierPath = String.format("%s/barrier_%s", barrierPrefix, version);
    final String barrierDonePath = String.format("%s/barrier_done", barrierPath);
    final String barrierProcessors = String.format("%s/barrier_processors", barrierPath);

    zkUtils.makeSurePersistentPathsExists(new String[]{barrierPrefix, barrierPath, barrierProcessors, barrierDonePath});

    // subscribe for processor's list changes
    LOG.info("Subscribing for child changes at " + barrierProcessors);
    zkUtils.getZkClient().subscribeChildChanges(barrierProcessors,
        new ZkBarrierChangeHandler(version, processorsNames));

    setTimer(version, BARRIER_TIMED_OUT_MS);
  }

  @Override
  public void waitForBarrier(String version, String processorsName, Runnable callback) {
    // if participant makes this call it means it has already stopped the old container and got the new job model.
    final String barrierPath = String.format("%s/barrier_%s", barrierPrefix, version);
    final String barrierDonePath = String.format("%s/barrier_done", barrierPath);
    final String barrierProcessors = String.format("%s/barrier_processors", barrierPath);
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
      // Find out the event & Log
      boolean allIn = true;

      if (currentChildren == null) {
        LOG.info("Got handleChildChange with null currentChildren");
        return;
      }

      LOG.info("list of children in the barrier = " + parentPath + ":" + Arrays.toString(currentChildren.toArray()));
      LOG.info("list of children to compare against = " + parentPath + ":" + Arrays.toString(names.toArray()));

      // check if all the names are in
      for (String name : names) {
        if (!currentChildren.contains(name)) {
          LOG.info("node " + name + " is still not in the list ");
          allIn = false;
          break;
        }
      }
      if (allIn) {
        LOG.info("ALl nodes reached the barrier");
        final String barrierPath = String.format("%s/barrier_%s", barrierPrefix, version);
        final String barrierDonePath = String.format("%s/barrier_done", barrierPath);
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
      } else if(done.equals(BARRIER_TIMED_OUT)) {
        // timed out
        LOG.error("Barrier for " + dataPath + " timed out");
        System.out.println("Barrier for " + dataPath + " timed out");
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
