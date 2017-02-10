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

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkBarrierForVersionUpgrade implements BarrierForVersionUpgrade {
  private final ZkUtils zkUtils;
  private final ZkKeyBuilder keyBuilder;
  private final static String BARRIER_DONE = "done";
  private final static Logger LOG = LoggerFactory.getLogger(ZkBarrierForVersionUpgrade.class);

  private final ScheduleAfterDebounceTime debounceTimer;

  final private String barrierPrefix;

  public ZkBarrierForVersionUpgrade(ZkUtils zkUtils, ScheduleAfterDebounceTime debounceTimer) {
    this.zkUtils = zkUtils;
    keyBuilder = zkUtils.getKeyBuilder();

    barrierPrefix = keyBuilder.getJobModelVersionBarrierPrefix();
    this.debounceTimer = debounceTimer;
  }

  @Override
  public void leaderStartBarrier(String version, List<String> processorsNames) {
    String barrierPath = String.format("%s/barrier_%s", barrierPrefix, version);
    String barrierDonePath = String.format("%s/barrier_done", barrierPath);
    String barrierProcessors = String.format("%s/barrier_processors", barrierPath);
    String barrier = String.format("%s/%s/barrier", barrierPrefix, version);

    // TODO - do we need a check if it exists - it needs to be deleted?
    zkUtils.makeSurePersistentPathsExists(new String[]{barrierPrefix, barrierPath, barrierProcessors, barrierDonePath});

    // callback for when the barrier is reached
    Runnable callback = new Runnable() {
      @Override
      public void run() {
        LOG.info("Writing BARRIER DONE to " + barrierDonePath);
        zkUtils.getZkClient().writeData(barrierDonePath, BARRIER_DONE);
      }
    };
    // subscribe for processor's list changes
    LOG.info("Subscribing for child changes at " + barrierProcessors);
    zkUtils.getZkClient().subscribeChildChanges(barrierProcessors,
        new ZkBarrierChangeHandler(callback, processorsNames));
  }

  @Override
  public void waitForBarrier(String version, String processorsName, Runnable callback) {
    // if participant makes this call it means it has already stopped the old container and got the new job model.
    String barrierPath = String.format("%s/barrier_%s", barrierPrefix, version);
    String barrierDonePath = String.format("%s/barrier_done", barrierPath);
    String barrierProcessors = String.format("%s/barrier_processors", barrierPath);
    String barrierProcessorThis = String.format("%s/%s", barrierProcessors, processorsName);


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
    Runnable callback;
    List<String> names;

    public ZkBarrierChangeHandler(Runnable callback, List<String> names) {
      this.callback = callback;
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
      // debug
      StringBuilder sb = new StringBuilder();
      for (String child : currentChildren) {
        sb.append(child).append(",");
      }
      LOG.info("list of children in the barrier = " + parentPath + ":" + sb.toString());
      sb = new StringBuilder();
      for (String child : names) {
        sb.append(child).append(",");
      }
      LOG.info("list of children to compare against = " + parentPath + ":" + sb.toString());


      // check if all the names are in
      for (String n : names) {
        if (!currentChildren.contains(n)) {
          LOG.info("node " + n + " is still not in the list ");
          allIn = false;
          break;
        }
      }
      if (allIn) {
        LOG.info("ALl nodes reached the barrier");
        callback.run(); // all the names have registered
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
      } else {
        // TODO do we need to resubscribe?
      }
    }

    @Override
    public void handleDataDeleted(String dataPath)
        throws Exception {
      LOG.warn("barrier done got deleted at " + dataPath);
    }
  }
}
