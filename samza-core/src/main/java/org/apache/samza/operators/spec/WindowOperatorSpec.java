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

package org.apache.samza.operators.spec;

import com.google.common.collect.ImmutableMap;
import org.apache.samza.operators.functions.FoldLeftFunction;
import org.apache.samza.operators.functions.WatermarkFunction;
import org.apache.samza.operators.impl.store.TimeSeriesKeySerde;
import org.apache.samza.operators.triggers.AnyTrigger;
import org.apache.samza.operators.triggers.RepeatingTrigger;
import org.apache.samza.operators.triggers.TimeBasedTrigger;
import org.apache.samza.operators.triggers.Trigger;
import org.apache.samza.operators.util.MathUtils;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.internal.WindowInternal;
import org.apache.samza.serializers.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * The spec for an operator that groups messages into finite windows for processing
 *
 * @param <M>  the type of input message to the window
 * @param <WK>  the type of key of the window
 * @param <WV>  the type of aggregated value in the window output {@link WindowPane}
 */
public class WindowOperatorSpec<M, WK, WV> extends OperatorSpec<M, WindowPane<WK, WV>> implements StatefulOperatorSpec {

  private static final Logger LOG = LoggerFactory.getLogger(WindowOperatorSpec.class);
  private final WindowInternal<M, WK, WV> window;

  /**
   * Constructor for {@link WindowOperatorSpec}.
   *
   * @param window  the window function
   * @param opId  auto-generated unique ID of this operator
   */
  WindowOperatorSpec(WindowInternal<M, WK, WV> window, int opId) {
    super(OpCode.WINDOW, opId);
    this.window = window;
  }

  public WindowInternal<M, WK, WV> getWindow() {
    return window;
  }

  /**
   * Get the default triggering interval for this {@link WindowOperatorSpec}
   *
   * This is defined as the GCD of all triggering intervals across all {@link TimeBasedTrigger}s configured for
   * this {@link WindowOperatorSpec}.
   *
   * @return the default triggering interval
   */
  public long getDefaultTriggerMs() {
    List<TimeBasedTrigger> timerTriggers = new ArrayList<>();

    if (window.getDefaultTrigger() != null) {
      timerTriggers.addAll(getTimeBasedTriggers(window.getDefaultTrigger()));
    }
    if (window.getEarlyTrigger() != null) {
      timerTriggers.addAll(getTimeBasedTriggers(window.getEarlyTrigger()));
    }
    if (window.getLateTrigger() != null) {
      timerTriggers.addAll(getTimeBasedTriggers(window.getLateTrigger()));
    }

    LOG.info("Got {} timer triggers", timerTriggers.size());

    List<Long> candidateDurations = timerTriggers.stream()
        .map(timeBasedTrigger -> timeBasedTrigger.getDuration().toMillis())
        .collect(Collectors.toList());

    return MathUtils.gcd(candidateDurations);
  }

  private List<TimeBasedTrigger> getTimeBasedTriggers(Trigger rootTrigger) {
    List<TimeBasedTrigger> timeBasedTriggers = new ArrayList<>();
    // traverse all triggers in the graph starting at the root trigger
    if (rootTrigger instanceof TimeBasedTrigger) {
      timeBasedTriggers.add((TimeBasedTrigger) rootTrigger);
    } else if (rootTrigger instanceof RepeatingTrigger) {
      // recurse on the underlying trigger
      timeBasedTriggers.addAll(getTimeBasedTriggers(((RepeatingTrigger) rootTrigger).getTrigger()));
    } else if (rootTrigger instanceof AnyTrigger) {
      List<Trigger> subTriggers = ((AnyTrigger) rootTrigger).getTriggers();

      for (Trigger subTrigger: subTriggers) {
        // recurse on each sub-trigger
        timeBasedTriggers.addAll(getTimeBasedTriggers(subTrigger));
      }
    }
    return timeBasedTriggers;
  }

  @Override
  public WatermarkFunction getWatermarkFn() {
    FoldLeftFunction fn = window.getFoldLeftFunction();
    return fn instanceof WatermarkFunction ? (WatermarkFunction) fn : null;
  }

  @Override
  public Collection<StoreDescriptor> getStoreDescriptors() {
    String storeName = getOpName();
    String storeFactory = "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory";
    long ttlMs = window.getTtlMs();

    Serde storeKeySerde = new TimeSeriesKeySerde<>(window.getKeySerde());
    Serde storeValSerde = window.getFoldLeftFunction() == null ? window.getMsgSerde() : window.getWindowValSerde();

    Map<String, String> otherProperties = ImmutableMap.of(
        String.format("stores.%s.rocksdb.ttl.ms", storeName), Long.toString(ttlMs),
        String.format("stores.%s.changelog.kafka.cleanup.policy", storeName), "delete",
        String.format("stores.%s.changelog.kafka.retention.ms", storeName), Long.toString(ttlMs));

    StoreDescriptor descriptor = new StoreDescriptor(storeName, storeFactory, storeKeySerde, storeValSerde, storeName, otherProperties);
    return Collections.singletonList(descriptor);
  }
}
