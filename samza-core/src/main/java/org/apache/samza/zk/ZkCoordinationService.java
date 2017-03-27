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

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.coordinator.BarrierForVersionUpgrade;
import org.apache.samza.coordinator.CoordinationService;
import org.apache.samza.coordinator.Latch;
import org.apache.samza.coordinator.LeaderElector;


public class ZkCoordinationService implements CoordinationService {
  public final ZkConfig zkConfig;
  public final ZkUtils zkUtils;
  public final String processorIdStr;
  public final ScheduleAfterDebounceTime debounceTimer;

  public ZkCoordinationService(String processorId, ZkConfig zkConfig, ZkUtils zkUtils, ScheduleAfterDebounceTime debounceTimer) {
    this.zkConfig = zkConfig;
    this.zkUtils = zkUtils;
    this.processorIdStr = processorId;
    this.debounceTimer = debounceTimer;
  }

  @Override
  public void start() {
    zkUtils.connect();
  }

  @Override
  public void stop() {
    zkUtils.close();
  }

  @Override
  public void reset() {
    zkUtils.deleteRoot();
  }

  @Override
  public LeaderElector getLeaderElector() {
    return new ZkLeaderElector(processorIdStr, zkUtils, debounceTimer);
  }

  @Override
  public Latch getLatch(int size, String latchId) {
    return new ZkProcessorLatch(size, latchId, processorIdStr, zkConfig, zkUtils);
  }

  @Override
  public BarrierForVersionUpgrade getBarrier(String barrierId, String version, List<String> participatns) {
    return new ZkBarrierForVersionUpgrade(barrierId, zkUtils, debounceTimer, version, participatns, zkConfig.getZkBarrierTimeoutMs());
  }
  @VisibleForTesting
  public ZkUtils getZkUtils() {
    return zkUtils;
  }
}
