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
import org.apache.samza.message.WatermarkMessage;
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
 */
public class WatermarkManager {
  private static final Logger log = LoggerFactory.getLogger(WatermarkManager.class);
  public static final long WATERMARK_NOT_EXIST = -1;

  private final String taskName;
  private final Map<SystemStreamPartition, WatermarkState> watermarkStates;
  private final Map<SystemStream, Long> watermarkPerStream;
  private final StreamMetadataCache metadataCache;
  private final Multimap<SystemStream, String> streamToTasks;
  private final MessageCollector collector;

  public WatermarkManager(String taskName,
      Multimap<SystemStream, String> streamToTasks,
      Set<SystemStreamPartition> ssps,
      StreamMetadataCache metadataCache,
      MessageCollector collector) {
    this.taskName = taskName;
    this.watermarkPerStream = new HashMap<>();
    this.streamToTasks = streamToTasks;
    this.metadataCache = metadataCache;
    this.collector = collector;

    Map<SystemStreamPartition, WatermarkState> states = new HashMap<>();
    ssps.forEach(ssp -> {
        states.put(ssp, new WatermarkState());
        watermarkPerStream.put(ssp.getSystemStream(), WATERMARK_NOT_EXIST);
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
    state.update(message.getTimestamp(), message.getTaskName(), message.getTaskCount());

    if (state.getWatermarkTime() != WATERMARK_NOT_EXIST) {
      long minTimestamp = watermarkStates.entrySet().stream()
          .filter(entry -> entry.getKey().getSystemStream().equals(ssp.getSystemStream()))
          .map(entry -> entry.getValue().getWatermarkTime())
          .min(Long::compare)
          .get();
      Long curWatermark = watermarkPerStream.get(ssp.getSystemStream());
      if (curWatermark == null || curWatermark < minTimestamp) {
        watermarkPerStream.put(ssp.getSystemStream(), minTimestamp);
        return createWatermark(minTimestamp, ssp.getSystemStream());
      }
    }

    return null;
  }

  public long getWatermarkTime(SystemStreamPartition ssp) {
    return watermarkStates.get(ssp).getWatermarkTime();
  }

  /**
   * Send the watermark message to all partitions of an intermediate stream
   * @param timestamp watermark timestamp
   * @param systemStream intermediate stream
   * @param taskCount total number of producer tasks
   */
  public void sendWatermark(long timestamp, SystemStream systemStream, int taskCount) {
    log.info("Send end-of-stream messages to all partitions of " + systemStream);
    final WatermarkMessage watermarkMessage = new WatermarkMessage(timestamp, taskName, taskCount);
    ControlMessageUtils.sendControlMessage(watermarkMessage, systemStream, metadataCache, collector);
  }

  public Watermark createWatermark(long timestamp, SystemStream systemStream) {
    return new WatermarkImpl(timestamp, systemStream);
  }

  /* package private */
  Multimap<SystemStream, String> getStreamToTasks() {
    return streamToTasks;
  }

  /**
   * Per ssp state of the watermarks. This class keeps track of the latest watermark timestamp
   * from each upstream producer tasks, and use the min to update the aggregated watermark time.
   */
  final static class WatermarkState {
    private int expectedTotal = Integer.MAX_VALUE;
    private final Map<String, Long> timestamps = new HashMap<>();
    private long watermarkTime = WATERMARK_NOT_EXIST;

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
  public final class WatermarkImpl implements Watermark {
    private final long timestamp;
    private final SystemStream systemStream;

    WatermarkImpl(long timestamp, SystemStream systemStream) {
      this.timestamp = timestamp;
      this.systemStream = systemStream;
    }

    @Override
    public long getTimestamp() {
      return timestamp;
    }

    @Override
    public SystemStream getSystemStream() {
      return systemStream;
    }

    public WatermarkManager getManager() {
      return WatermarkManager.this;
    }
  }

  /**
   * Build a watermark control message envelope for an ssp of a source input.
   * @param timestamp watermark time
   * @param ssp {@link SystemStreamPartition} where the watermark coming from.
   * @return envelope of the watermark control message
   */
  public static IncomingMessageEnvelope buildWatermarkEnvelope(long timestamp, SystemStreamPartition ssp) {
    return new IncomingMessageEnvelope(ssp, null, "", new WatermarkMessage(timestamp, null, 0));
  }
}
