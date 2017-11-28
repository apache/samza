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

package org.apache.samza.system.kinesis.metrics;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;

import com.amazonaws.services.kinesis.model.Record;


/**
 * KinesisSystemConsumerMetrics class has per-stream metrics and aggregate metrics across kinesis consumers
 */

public class KinesisSystemConsumerMetrics {

  private final MetricsRegistry registry;

  // Aggregate metrics across all kinesis system consumers
  private static Counter aggEventReadRate = null;
  private static Counter aggEventByteReadRate = null;
  private static SamzaHistogram aggReadLatency = null;
  private static SamzaHistogram aggMillisBehindLatest = null;

  // Per-stream metrics
  private Map<String, Counter> eventReadRates;
  private Map<String, Counter> eventByteReadRates;
  private Map<String, SamzaHistogram> readLatencies;
  private Map<String, SamzaHistogram> millisBehindLatest;

  private static final Object LOCK = new Object();

  private static final String AGGREGATE = "aggregate";
  private static final String EVENT_READ_RATE = "eventReadRate";
  private static final String EVENT_BYTE_READ_RATE = "eventByteReadRate";
  private static final String READ_LATENCY = "readLatency";
  private static final String MILLIS_BEHIND_LATEST = "millisBehindLatest";

  public KinesisSystemConsumerMetrics(MetricsRegistry registry) {
    this.registry = registry;
  }

  public void initializeMetrics(Set<String> streamNames) {
    eventReadRates = streamNames.stream()
        .collect(Collectors.toConcurrentMap(Function.identity(), x -> registry.newCounter(x, EVENT_READ_RATE)));
    eventByteReadRates = streamNames.stream()
        .collect(Collectors.toConcurrentMap(Function.identity(), x -> registry.newCounter(x, EVENT_BYTE_READ_RATE)));
    readLatencies = streamNames.stream()
        .collect(Collectors.toConcurrentMap(Function.identity(), x -> new SamzaHistogram(registry, x, READ_LATENCY)));
    millisBehindLatest = streamNames.stream()
        .collect(Collectors.toConcurrentMap(Function.identity(),
            x -> new SamzaHistogram(registry, x, MILLIS_BEHIND_LATEST)));

    // Locking to ensure that these aggregated metrics will be created only once across multiple system consumers.
    synchronized (LOCK) {
      if (aggEventReadRate == null) {
        aggEventReadRate = registry.newCounter(AGGREGATE, EVENT_READ_RATE);
        aggEventByteReadRate = registry.newCounter(AGGREGATE, EVENT_BYTE_READ_RATE);
        aggReadLatency = new SamzaHistogram(registry, AGGREGATE, READ_LATENCY);
        aggMillisBehindLatest = new SamzaHistogram(registry, AGGREGATE, MILLIS_BEHIND_LATEST);
      }
    }
  }

  public void updateMillisBehindLatest(String stream, Long millisBehindLatest) {
    this.millisBehindLatest.get(stream).update(millisBehindLatest);
    aggMillisBehindLatest.update(millisBehindLatest);
  }

  public void updateMetrics(String stream, Record record) {
    eventReadRates.get(stream).inc();
    aggEventReadRate.inc();

    long recordSize = record.getData().array().length + record.getPartitionKey().length();
    eventByteReadRates.get(stream).inc(recordSize);
    aggEventByteReadRate.inc(recordSize);

    long latencyMs = Duration.between(Instant.now(), record.getApproximateArrivalTimestamp().toInstant()).toMillis();
    readLatencies.get(stream).update(latencyMs);
    aggReadLatency.update(latencyMs);
  }
}
