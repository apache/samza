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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.WatermarkMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages the watermarks coming from input/intermediate streams in a task. Internally it keeps track
 * of the latest watermark timestamp from each upstream task, and use the min as the consolidated watermark time.
 * Lagging tasks can be excluded from the calculation by configuring "task.watermark.idle.timeout.ms", and watermarks
 * will be computed from the other tasks. This will help unblock downstream aggregations, but at the risk of advancing
 * event-time clock faster and events coming later from lagging tasks will become late arrivals.
 *
 * This class is thread-safe. However, having parallelism within a task may result in out-of-order processing
 * and inaccurate watermarks. In this scenario, watermarks might be emitted before the previous messages fully processed.
 */
class WatermarkStates {
  private static final Logger LOG = LoggerFactory.getLogger(WatermarkStates.class);

  public static final long WATERMARK_NOT_EXIST = -1;

  private final static class WatermarkState {
    private final int expectedTotal;
    private final Map<String, Long> timestamps = new HashMap<>();
    private final Map<String, Long> lastUpdateTime = new HashMap<>();
    private final long watermarkIdleTimeout;
    private final int quorumSize;
    private final long createTime;
    private final LongSupplier systemTimeFunc;
    private volatile long watermarkTime = WATERMARK_NOT_EXIST;
    private volatile int quorumCount = 0;

    WatermarkState(
            int expectedTotal,
            long watermarkIdleTimeout,
            double watermarkQuorumSizePercentage,
            LongSupplier systemTimeFunc) {
      this.expectedTotal = expectedTotal;
      this.watermarkIdleTimeout = watermarkIdleTimeout;
      this.quorumSize = (int) (expectedTotal * watermarkQuorumSizePercentage);
      this.systemTimeFunc = systemTimeFunc;
      this.createTime = systemTimeFunc.getAsLong();
    }

    synchronized void update(long timestamp, String taskName) {
      long currentTime = systemTimeFunc.getAsLong();
      if (taskName != null) {
        Long ts = timestamps.get(taskName);
        if (ts != null && ts > timestamp) {
          LOG.warn(String.format("Incoming watermark %s is smaller than existing watermark %s for upstream task %s",
              timestamp, ts, taskName));
        } else {
          timestamps.put(taskName, timestamp);
          lastUpdateTime.put(taskName, currentTime);
        }
      }

      if (taskName == null) {
        // we get watermark either from the source or from the aggregator task
        watermarkTime = Math.max(watermarkTime, timestamp);
      } else if (canUpdateWatermark(currentTime)) {
        final long minWatermark;
        if (watermarkIdleTimeout <= 0) {
          // All upstream tasks are required in the computation
          minWatermark = timestamps.values().stream().min(Long::compare).orElse(timestamp);
        } else {
          // Exclude the tasks that have been idle in watermark emission.
          long min = Long.MAX_VALUE;
          long watermarkIdleThreshold = currentTime - watermarkIdleTimeout;
          int updateCount = 0;
          for (Map.Entry<String, Long> entry : timestamps.entrySet()) {
            // Check the update happens before the idle timeout
            if (lastUpdateTime.get(entry.getKey()) > watermarkIdleThreshold) {
              min = Math.min(min, entry.getValue());
              updateCount++;
            }
          }

          // Active tasks must exceed the quorum size
          minWatermark = (updateCount >= quorumSize && min != Long.MAX_VALUE) ? min : WATERMARK_NOT_EXIST;

          // Log the current quorum count
          if (this.quorumCount != updateCount) {
            this.quorumCount = updateCount;
            LOG.info("Current quorum count is {} for watermark aggregation, and the expected quorum size is {}",
                    this.quorumCount, this.quorumSize);
          }
        }
        watermarkTime = Math.max(watermarkTime, minWatermark);
      }
    }

    private boolean canUpdateWatermark(long currentTime) {
      // The watermark can be updated if
      // 1. we received watermarks from all upstream tasks, or
      // 2. we allow task idle in emitting watermarks and the idle time has passed.
      return (timestamps.size() == expectedTotal)
              // Handle the case we didn't receive the watermarks from some tasks since startup
              || (watermarkIdleTimeout > 0 && currentTime - createTime > watermarkIdleTimeout);
    }

    long getWatermarkTime() {
      return watermarkTime;
    }
  }

  private final Map<SystemStreamPartition, WatermarkState> watermarkStates;
  private final List<SystemStreamPartition> intermediateSsps;
  private final WatermarkMetrics watermarkMetrics;

  WatermarkStates(
          Set<SystemStreamPartition> ssps,
          Map<SystemStream, Integer> producerTaskCounts,
          MetricsRegistry metricsRegistry,
          long watermarkIdleTimeout,
          double watermarkQuorumSizePercentage) {
    this(ssps, producerTaskCounts, metricsRegistry, watermarkIdleTimeout,
            watermarkQuorumSizePercentage, System::currentTimeMillis);
  }

  //Internal: test-only
  WatermarkStates(
      Set<SystemStreamPartition> ssps,
      Map<SystemStream, Integer> producerTaskCounts,
      MetricsRegistry metricsRegistry,
      long watermarkIdleTimeout,
      double watermarkQuorumSizePercentage,
      LongSupplier systemTimeFunc) {
    final Map<SystemStreamPartition, WatermarkState> states = new HashMap<>();
    final List<SystemStreamPartition> intSsps = new ArrayList<>();

    ssps.forEach(ssp -> {
      final int producerCount = producerTaskCounts.getOrDefault(ssp.getSystemStream(), 0);
      states.put(ssp,
              new WatermarkState(producerCount, watermarkIdleTimeout, watermarkQuorumSizePercentage, systemTimeFunc));
      if (producerCount != 0) {
        intSsps.add(ssp);
      }
    });
    this.watermarkStates = Collections.unmodifiableMap(states);
    this.watermarkMetrics = new WatermarkMetrics(metricsRegistry);
    this.intermediateSsps = Collections.unmodifiableList(intSsps);
  }

  /**
   * Update the state upon receiving a watermark message.
   * @param watermarkMessage message of {@link WatermarkMessage}
   * @param ssp system stream partition
   * @return true iff the stream has a new watermark
   */
  void update(WatermarkMessage watermarkMessage, SystemStreamPartition ssp) {
    WatermarkState state = watermarkStates.get(ssp);
    if (state != null) {
      state.update(watermarkMessage.getTimestamp(), watermarkMessage.getTaskName());
    } else {
      LOG.error("SSP {} doesn't have watermark states", ssp);
    }
  }

  long getWatermark(SystemStream systemStream) {
    return watermarkStates.entrySet().stream()
        .filter(entry -> entry.getKey().getSystemStream().equals(systemStream))
        .map(entry -> entry.getValue().getWatermarkTime())
        .min(Long::compare)
        .orElse(WATERMARK_NOT_EXIST);
  }

  /* package private for testing */
  long getWatermarkPerSSP(SystemStreamPartition ssp) {
    return watermarkStates.get(ssp).getWatermarkTime();
  }

  void updateAggregateMetric(SystemStreamPartition ssp, long time) {
    if (intermediateSsps.contains(ssp)) {
      // Only report the aggregates watermarks for intermediate streams
      // to reduce the amount of metrics
      watermarkMetrics.setAggregateTime(ssp, time);
    }
  }
}
