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

package org.apache.samza.operators.windows;

import org.apache.samza.operators.data.MessageEnvelope;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * Programmer API for specifying {@link Triggers}s for a {@link Window}.
 *
 * <p> A {@link Triggers} instance specifies all the early and late triggers for a {@link Window}. It also specifies how
 * window results relate over time. Window results can be accumulating (where every result builds over its previous ones) or
 * discarding (where every result is independent).
 *
 * @param <M> type of input {@link MessageEnvelope} in the window
 * @param <K> type of key in {@link MessageEnvelope}
 * @param <V> type of value in the {@link MessageEnvelope}
 *
 * <p> The below example windows an input into tumbling windows of 10s, and emits early results periodically every 4s in
 * processing time, or for every 50 messages. It also specifies that window results are accumulating.
 *
 * <pre> {@code
 * MessageStream<> windowedStream = stream.window(
 *   Windows.tumblingWindow(Time.of(10, TimeUnit.SECONDS))
 *     .setTriggers(new TriggersBuilder<>()
 *       .withEarlyFiringsAfterCountAtleast(50)
 *       .withEarlyFiringsEvery(Time.of(4, TimeUnit.SECONDS))
 *       .accumulateFiredPanes()
 *       .build());
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


  static class Trigger<K, V> {
    private final TriggerType type;

    Trigger(TriggerType type) {
      this.type = type;
    }
  }

  /*
   * Defines a {@link Trigger} that triggers periodically based on the specified time delay.
   *
   */
  static class PeriodicTimeTrigger extends Trigger {
    private final long delayMillis;
    private final Time.TimeCharacteristic timeCharacteristic;

    PeriodicTimeTrigger(TriggerType type, long delayMillis, Time.TimeCharacteristic timeCharacteristic) {
      super(type);
      this.delayMillis = delayMillis;
      this.timeCharacteristic = timeCharacteristic;
    }

    public long getDelayMillis() {
      return delayMillis;
    }

    public Time.TimeCharacteristic getTimeCharacteristic() {
      return timeCharacteristic;
    }
  }

  /*
   * Defines a {@link Trigger} that triggers when there is no message in a {@link Window} for the specified gap duration.
   */
  static class TimeSinceLastMessageTrigger extends Trigger {
    private final long gapMillis;
    private final Time.TimeCharacteristic timeCharacteristic;

    TimeSinceLastMessageTrigger(TriggerType type, long gapMillis, Time.TimeCharacteristic timeCharacteristic) {
      super(type);
      this.gapMillis = gapMillis;
      this.timeCharacteristic = timeCharacteristic;
    }

    public long getGapMillis() {
      return gapMillis;
    }

    public Time.TimeCharacteristic getTimeCharacteristic() {
      return timeCharacteristic;
    }
  }

  /*
   * Defines a {@link Trigger} that triggers based on the count of messages in the {@link Window}.
   */

  static class CountTrigger extends Trigger {
    private final long count;

    CountTrigger(TriggerType type , long count) {
      super(type);
      this.count = count;
    }

    public long getCount() {
      return count;
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

    private Triggers(List<Trigger> earlyTriggers, List<Trigger> lateTriggers, AccumulationMode mode) {
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
   * Add an early trigger that periodically emits results after a processing time delay. Used to
   * emit an early result before all data for the {@link Window} have arrived.
   *
   * @param delay the delay period between firings
   * @return a reference to this object
   */
  public TriggersBuilder withEarlyFiringsEvery(Time delay) {
    earlyTriggers.add(new PeriodicTimeTrigger(TriggerType.EARLY, delay.toMilliseconds(), Time.TimeCharacteristic.PROCESSING_TIME));
    return this;
  }

  /**
   * Add a late trigger that periodically emits results after a processing time delay. Used to emit
   * a results for late arrivals.
   *
   * @param delay the delay period between firings
   * @return a reference to this object
   */
  public TriggersBuilder withLateFiringsEvery(Time delay) {
    lateTriggers.add(new PeriodicTimeTrigger(TriggerType.LATE, delay.toMilliseconds(), Time.TimeCharacteristic.PROCESSING_TIME));
    return this;
  }

  /**
   * Add an early trigger that emits results after every {@code count} messages. Used to
   * emit an early result before all data for the {@link Window} have arrived.
   *
   * @param count the number of messages
   * @return a reference to this object
   */
  public TriggersBuilder withEarlyFiringsAfterCountAtleast(long count) {
    earlyTriggers.add(new CountTrigger(TriggerType.EARLY, count));
    return this;
  }

  /**
   * Add a late trigger that periodically emits results after a certain number of messages in the window.
   *
   * @param numMessages the delay period between firings
   * @return a reference to this object
   */

  public TriggersBuilder withLateFiringsAfterCountAtleast(long numMessages) {
    lateTriggers.add(new CountTrigger(TriggerType.LATE, numMessages));
    return this;
  }

  /**
   * Specifies that previously fired results should be accumulated. This is applicable when each window result builds
   * on the previous ones.
   *
   * @return a reference to this object
   */
  public TriggersBuilder accumulateFiredPanes() {
    this.accumulationMode = AccumulationMode.ACCUMULATING;
    return this;
  }

  /**
   * Specifies that previously fired results should be discarded. This is applicable when each window result is
   * independent.
   *
   * @return a reference to this object
   */
  public TriggersBuilder discardFiredPanes() {
    this.accumulationMode = AccumulationMode.DISCARDING;
    return this;
  }

  /**
   * Build an immutable {@link Triggers} instance from this {@link TriggersBuilder}
   *
   * @return a reference to this object
   */
  public Triggers build() {
    return new Triggers(Collections.unmodifiableList(earlyTriggers), Collections.unmodifiableList(lateTriggers),
      accumulationMode);
  }
}
