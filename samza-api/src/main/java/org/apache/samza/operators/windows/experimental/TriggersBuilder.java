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

package org.apache.samza.operators.windows.experimental;

import org.apache.samza.operators.data.MessageEnvelope;

import java.util.ArrayList;
import java.util.List;


/**
 * Programmer API for specifying {@link Triggers}s for a {@link Window}.
 *
 * A {@link Triggers} instance specifies all the early and late triggers for a {@link Window}. It also specifies how results
 * from a window relate over time - whether previously fired window results are accumulating (where every result builds
 * over its previous ones) or discarding (where every result is independent).
 *
 * @param <M> type of input {@link MessageEnvelope} in the window
 * @param <K> type of key in {@link MessageEnvelope}
 * @param <V> type of value in the {@link MessageEnvelope}
 *
 * The below example windows an input into tumbling windows of 10s, and emits early results periodically every 4s in
 * processing time, or for every 50 messages. It also specifies that window results are accumulating.
 *
 * <pre>
 *   {@code
 *    MessageStream<> windowedStream = stream.window(Windows.tumblingWindow(10000)
 *      setTriggers(new Triggers<>()
 *       .withEarlyFiringsAfterCountAtleast(50)
 *       .withEarlyFiringsEvery(4000)
 *       .accumulateFiredPanes().build());
 *    }
 *  </pre>
 */
public final class TriggersBuilder<M extends MessageEnvelope, K, V> {

  /*
   * Is this an early, late or a default trigger.
   */
  enum TriggerType {
    EARLY, LATE, DEFAULT
  }

  /*
   * Should the trigger fire in event time or processing time.
   */
  enum TimeCharacteristic {
    EVENT_TIME, PROCESSING_TIME
  }

  static class Trigger<K, V> {
    private final TriggerType type;

    Trigger(TriggerType type) {
      this.type = type;
    }
  }

  /*
   * Defines a {@link Trigger} that triggers periodically based on the specified time duration.
   *
   */
  static class PeriodicTimeTrigger extends Trigger {
    private final long durationMs;
    private final TimeCharacteristic timeCharacteristic;

    PeriodicTimeTrigger(TriggerType type, long duration, TimeCharacteristic timeCharacteristic) {
      super(type);
      this.durationMs = duration;
      this.timeCharacteristic = timeCharacteristic;
    }
  }

  /*
   * Defines a {@link Trigger} that triggers when there is no message in a {@link Window} for the specified gap duration.
   *  A late trigger specifies handling of
   * late arrivals (caused typically due to skews, and upstream delays) in the data.
   */
  static class TimeSinceLastMessageTrigger extends Trigger {
    private final long gapMs;
    private final TimeCharacteristic timeCharacteristic;

    TimeSinceLastMessageTrigger(TriggerType type, long timeout, TimeCharacteristic timeCharacteristic) {
      super(type);
      this.gapMs = timeout;
      this.timeCharacteristic = timeCharacteristic;
    }
  }

  /*
   * Defines a {@link Trigger} that triggers based on the count of messages in the window.
   */

  static class CountTrigger extends Trigger {
    private final long count;
    CountTrigger(TriggerType type , long count) {
      super(type);
      this.count = count;
    }
  }


  private final List<Trigger> earlyTriggers = new ArrayList<>();
  private final List<Trigger> lateTriggers = new ArrayList<>();

  private AccumulationMode accumulationMode = AccumulationMode.ACCUMULATING;

  private enum AccumulationMode {
    ACCUMULATING, DISCARDING
  }

  static class Triggers {

    /**
     * Early and late firings, accumulation modes for a {@link Window}
     */
    private final List<Trigger> earlyTriggers;
    private final List<Trigger> lateTriggers;
    private final AccumulationMode accumulationMode;

    Triggers(List<Trigger> earlyTriggers, List<Trigger> lateTriggers, AccumulationMode mode) {
      this.earlyTriggers = earlyTriggers;
      this.lateTriggers = lateTriggers;
      this.accumulationMode = mode;
    }

    List<Trigger> getEarlyTriggers() {
      return earlyTriggers;
    }

    List<Trigger> getLateTriggers() {
      return lateTriggers;
    }

    AccumulationMode getAccumulationMode() {
      return accumulationMode;
    }
  }

  /**
   * Add an early trigger that periodically emits results after a processing time delay. An early trigger specifies
   * emission of an early result before all data for the {@link Window} have arrived.
   * @param period the delay period between firings
   */
  public TriggersBuilder withEarlyFiringsEvery(long period) {
    earlyTriggers.add(new PeriodicTimeTrigger(TriggerType.EARLY, period, TimeCharacteristic.PROCESSING_TIME));
    return this;
  }

  /**
   * Add a late trigger that periodically emits results after a processing time delay.
   * @param period the delay period between firings
   */
  public TriggersBuilder withLateFiringsEvery(long period) {
    lateTriggers.add(new PeriodicTimeTrigger(TriggerType.LATE, period, TimeCharacteristic.PROCESSING_TIME));
    return this;
  }

  /**
   * Add an early trigger that periodically emits results after a certain number of messages in the window.
   * @param numMessages the delay period between firings An early trigger specifies
   * emission of an early result before all data for the {@link Window} have arrived.
   */
  public TriggersBuilder withEarlyFiringsAfterCountAtleast(long numMessages) {
    earlyTriggers.add(new CountTrigger(TriggerType.EARLY, numMessages));
    return this;
  }

  /**
   * Add a late trigger that periodically emits results after a certain number of messages in the window.
   * @param numMessages the delay period between firings
   */

  public TriggersBuilder withLateFiringsAfterCountAtleast(long numMessages) {
    lateTriggers.add(new CountTrigger(TriggerType.LATE, numMessages));
    return this;
  }

  /**
   * Specifies that previously fired results should be accumulated. This is applicable when each window output builds
   * on the previous ones.
   */
  public TriggersBuilder accumulateFiredPanes() {
    this.accumulationMode = AccumulationMode.ACCUMULATING;
    return this;
  }

  /**
   * Specifies that previously fired results should be discarded. This is applicable when each window output is
   * independent.
   */
  public TriggersBuilder discardFiredPanes() {
    this.accumulationMode = AccumulationMode.DISCARDING;
    return this;
  }

  /**
   * Build an immutable {@link Triggers} from this {@link TriggersBuilder}
   */
  public Triggers build() {
    return new Triggers(earlyTriggers, lateTriggers, accumulationMode);
  }
}
