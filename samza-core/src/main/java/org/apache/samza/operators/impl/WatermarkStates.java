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

package org.apache.samza.operators.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.WatermarkMessage;


class WatermarkStates {
  public static final long TIME_NOT_EXIST = -1;
  public static final long TIME_MAX = Long.MAX_VALUE;

  /**
   * Per ssp state of the watermarks. This class keeps track of the latest watermark timestamp
   * from each upstream producer tasks, and use the min to update the aggregated watermark time.
   */
  private final static class WatermarkState {
    private int expectedTotal = Integer.MAX_VALUE;
    private final Map<String, Long> timestamps = new HashMap<>();
    private long watermarkTime = TIME_NOT_EXIST;

    WatermarkState(int expectedTotal) {
      this.expectedTotal = expectedTotal;
    }

    boolean update(long timestamp, String taskName) {
      final long preWatermarkTime = watermarkTime;
      if (taskName != null) {
        timestamps.put(taskName, timestamp);
      }

      if (timestamps.size() == expectedTotal) {
        Optional<Long> min = timestamps.values().stream().min(Long::compare);
        watermarkTime = min.orElse(timestamp);
      }

      return preWatermarkTime != watermarkTime;
    }

    long getWatermarkTime() {
      return watermarkTime;
    }
  }

  private final Map<SystemStreamPartition, WatermarkState> watermarkStates;
  private final Map<SystemStream, Long> watermarks = new ConcurrentHashMap<>();

  WatermarkStates(Set<SystemStreamPartition> ssps, Map<SystemStream, Integer> producerTaskCounts) {
    Map<SystemStreamPartition, WatermarkState> states = new HashMap<>();
    ssps.forEach(ssp -> {
        states.put(ssp, new WatermarkState(producerTaskCounts.getOrDefault(ssp.getSystemStream(), 0)));
        watermarks.put(ssp.getSystemStream(), TIME_NOT_EXIST);
      });
    this.watermarkStates = Collections.unmodifiableMap(states);
  }

  /**
   * documentation
   * @param watermarkMessage
   * @param ssp
   * @return boolean flag of whether the watermark of the stream has been updated
   */
  synchronized boolean update(WatermarkMessage watermarkMessage, SystemStreamPartition ssp) {
    WatermarkState state = watermarkStates.get(ssp);
    boolean updated = state.update(watermarkMessage.getTimestamp(), watermarkMessage.getTaskName());

    if (updated) {
      long minTimestamp = watermarkStates.entrySet().stream()
          .filter(entry -> entry.getKey().getSystemStream().equals(ssp.getSystemStream()))
          .map(entry -> entry.getValue().getWatermarkTime())
          .min(Long::compare)
          .get();
      Long curWatermark = watermarks.get(ssp.getSystemStream());
      if (curWatermark == null || curWatermark < minTimestamp) {
        watermarks.put(ssp.getSystemStream(), minTimestamp);
        return true;
      }
    }
    return false;
  }

  long getWatermark(SystemStream systemStream) {
    return watermarks.get(systemStream);
  }
}
