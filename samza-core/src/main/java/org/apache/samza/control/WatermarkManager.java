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

package org.apache.samza.control;

import com.google.common.collect.Multimap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.samza.system.WatermarkMessage;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class manages watermarks. It aggregates the watermark control messages from the upstage tasks
 * for each SSP into an envelope of {@link Watermark}, and provide a dispatcher to propagate it to downstream.
 *
 * Internal use only.
 */
public class WatermarkManager {
  private static final Logger log = LoggerFactory.getLogger(WatermarkManager.class);
  public static final long TIME_NOT_EXIST = -1;

  private final String taskName;
  private final Map<SystemStreamPartition, WatermarkState> watermarkStates;
  private final Map<SystemStream, Long> watermarkPerStream;
  private final StreamMetadataCache metadataCache;
  private final MessageCollector collector;
  // mapping from output stream to its upstream task count
  private final Map<SystemStream, Integer> upstreamTaskCounts;

  public WatermarkManager(String taskName,
      Multimap<SystemStream, String> inputToTasks,
      Set<SystemStreamPartition> ssps,
      StreamMetadataCache metadataCache,
      MessageCollector collector) {
    this.taskName = taskName;
    this.watermarkPerStream = new HashMap<>();
    this.metadataCache = metadataCache;
    this.collector = collector;
    this.upstreamTaskCounts = null;

    Map<SystemStreamPartition, WatermarkState> states = new HashMap<>();
    ssps.forEach(ssp -> {
        states.put(ssp, new WatermarkState());
        watermarkPerStream.put(ssp.getSystemStream(), TIME_NOT_EXIST);
      });
    this.watermarkStates = Collections.unmodifiableMap(states);
  }

  /**
   * Update the watermark based on the incoming watermark message. The message contains
   * a timestamp and the upstream producer task. The aggregation result is the minimal value
   * of all watermarks for the stream:
   * <ul>
   *   <li>Watermark(ssp) = min { Watermark(task) | task is upstream producer and the count equals total expected tasks } </li>
   *   <li>Watermark(stream) = min { Watermark(ssp) | ssp is a partition of stream that assigns to this task } </li>
   * </ul>
   *
   * @param envelope the envelope contains {@link WatermarkMessage}
   * @return watermark envelope if there is a new aggregate watermark for the stream
   */
  public Watermark update(IncomingMessageEnvelope envelope) {
    SystemStreamPartition ssp = envelope.getSystemStreamPartition();
    WatermarkState state = watermarkStates.get(ssp);
    WatermarkMessage message = (WatermarkMessage) envelope.getMessage();
    state.update(message.getTimestamp(), message.getTaskName(), 0);

    if (state.getWatermarkTime() != TIME_NOT_EXIST) {
      long minTimestamp = watermarkStates.entrySet().stream()
          .filter(entry -> entry.getKey().getSystemStream().equals(ssp.getSystemStream()))
          .map(entry -> entry.getValue().getWatermarkTime())
          .min(Long::compare)
          .get();
      Long curWatermark = watermarkPerStream.get(ssp.getSystemStream());
      if (curWatermark == null || curWatermark < minTimestamp) {
        watermarkPerStream.put(ssp.getSystemStream(), minTimestamp);
        return new WatermarkImpl(minTimestamp);
      }
    }

    return null;
  }

  /* package private */
  long getWatermarkTime(SystemStreamPartition ssp) {
    return watermarkStates.get(ssp).getWatermarkTime();
  }

  /**
   * Send the watermark message to all partitions of an intermediate stream
   * @param timestamp watermark timestamp
   * @param systemStream intermediate stream
   */
  void sendWatermark(long timestamp, SystemStream systemStream, int taskCount) {
    log.info("Send end-of-stream messages to all partitions of " + systemStream);
    final WatermarkMessage watermarkMessage = new WatermarkMessage(timestamp, taskName);
    //ControlMessageUtils.sendControlMessage(watermarkMessage, systemStream, metadataCache, collector);
  }

  /**
   * Per ssp state of the watermarks. This class keeps track of the latest watermark timestamp
   * from each upstream producer tasks, and use the min to update the aggregated watermark time.
   */
  final static class WatermarkState {
    private int expectedTotal = Integer.MAX_VALUE;
    private final Map<String, Long> timestamps = new HashMap<>();
    private long watermarkTime = TIME_NOT_EXIST;

    void update(long timestamp, String taskName, int taskCount) {
      if (taskName != null) {
        timestamps.put(taskName, timestamp);
      }
      expectedTotal = taskCount;

      if (timestamps.size() == expectedTotal) {
        Optional<Long> min = timestamps.values().stream().min(Long::compare);
        watermarkTime = min.orElse(timestamp);
      }
    }

    long getWatermarkTime() {
      return watermarkTime;
    }
  }

  /**
   * Implementation of the Watermark. It keeps a reference to the {@link WatermarkManager}
   */
  class WatermarkImpl implements Watermark {
    private final long timestamp;

    WatermarkImpl(long timestamp) {
      this.timestamp = timestamp;
    }

    @Override
    public long getTimestamp() {
      return timestamp;
    }

    @Override
    public void propagate(SystemStream systemStream) {
      sendWatermark(timestamp, systemStream, upstreamTaskCounts.get(systemStream));
    }

    @Override
    public Watermark copyWithTimestamp(long time) {
      return new WatermarkImpl(time);
    }
  }

}
