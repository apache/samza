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

import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.operators.functions.PartialJoinFunction;
import org.apache.samza.operators.functions.PartialJoinFunction.PartialJoinMessage;
import org.apache.samza.operators.spec.JoinOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.util.Clock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Implementation of one side of a {@link JoinOperatorSpec} that buffers and joins its input messages of
 * type {@code M} with buffered input messages of type {@code JM} in the paired {@link PartialJoinOperatorImpl}.
 *
 * @param <K> the type of join key
 * @param <M> the type of input messages on this side of the join
 * @param <JM> the type of input message on the other side of the join
 * @param <RM> the type of join result
 */
class PartialJoinOperatorImpl<K, M, JM, RM> extends OperatorImpl<M, RM> {

  private final JoinOperatorSpec<K, M, JM, RM> joinOpSpec;
  private final boolean isLeftSide; // whether this operator impl is for the left side of the join
  private final PartialJoinFunction<K, M, JM, RM> thisPartialJoinFn;
  private final PartialJoinFunction<K, JM, M, RM> otherPartialJoinFn;
  private final long ttlMs;
  private final Clock clock;

  private Counter keysRemoved;

  PartialJoinOperatorImpl(JoinOperatorSpec<K, M, JM, RM> joinOpSpec, boolean isLeftSide,
      PartialJoinFunction<K, M, JM, RM> thisPartialJoinFn,
      PartialJoinFunction<K, JM, M, RM> otherPartialJoinFn,
      Config config, TaskContext context, Clock clock) {
    this.joinOpSpec = joinOpSpec;
    this.isLeftSide = isLeftSide;
    this.thisPartialJoinFn = thisPartialJoinFn;
    this.otherPartialJoinFn = otherPartialJoinFn;
    this.ttlMs = joinOpSpec.getTtlMs();
    this.clock = clock;
  }

  @Override
  protected void handleInit(Config config, TaskContext context) {
    keysRemoved = context.getMetricsRegistry()
        .newCounter(OperatorImpl.class.getName(), getOperatorName() + "-keys-removed");
    this.thisPartialJoinFn.init(config, context);
  }

  @Override
  public Collection<RM> handleMessage(M message, MessageCollector collector, TaskCoordinator coordinator) {
    K key = thisPartialJoinFn.getKey(message);
    thisPartialJoinFn.getState().put(key, new PartialJoinMessage<>(message, clock.currentTimeMillis()));
    PartialJoinMessage<JM> otherMessage = otherPartialJoinFn.getState().get(key);
    long now = clock.currentTimeMillis();
    if (otherMessage != null && otherMessage.getReceivedTimeMs() > now - ttlMs) {
      RM joinResult = thisPartialJoinFn.apply(message, otherMessage.getMessage());
      return Collections.singletonList(joinResult);
    }
    return Collections.emptyList();
  }

  @Override
  public Collection<RM> handleTimer(MessageCollector collector, TaskCoordinator coordinator) {
    long now = clock.currentTimeMillis();

    KeyValueStore<K, PartialJoinMessage<M>> thisState = thisPartialJoinFn.getState();
    KeyValueIterator<K, PartialJoinMessage<M>> iterator = thisState.all();
    List<K> keysToRemove = new ArrayList<>();

    while (iterator.hasNext()) {
      Entry<K, PartialJoinMessage<M>> entry = iterator.next();
      if (entry.getValue().getReceivedTimeMs() < now - ttlMs) {
        keysToRemove.add(entry.getKey());
      } else {
        break; // InternalInMemoryStore uses a LinkedHashMap and will return entries in insertion order
      }
    }

    iterator.close();
    thisState.deleteAll(keysToRemove);
    keysRemoved.inc(keysToRemove.size());
    return Collections.emptyList();
  }

  @Override
  protected void handleClose() {
    this.thisPartialJoinFn.close();
  }

  protected OperatorSpec<M, RM> getOperatorSpec() {
    return (OperatorSpec<M, RM>) joinOpSpec;
  }

  /**
   * The name for this {@link PartialJoinOperatorImpl} that includes information about which
   * side of the join it is for.
   *
   * @return the {@link PartialJoinOperatorImpl} name.
   */
  @Override
  protected String getOperatorName() {
    String side = isLeftSide ? "L" : "R";
    return this.joinOpSpec.getOpName() + "-" + side;
  }
}
