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

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.operators.OpContext;
import org.apache.samza.operators.functions.PartialJoinFunction;
import org.apache.samza.operators.impl.store.TimestampedValue;
import org.apache.samza.operators.spec.JoinOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.util.Clock;

import java.util.Collection;
import java.util.Collections;

/**
 * Implementation of one side of a {@link JoinOperatorSpec} that buffers and joins its input messages of
 * type {@code M} with buffered input messages of type {@code OM} in the paired {@link PartialJoinOperatorImpl}.
 *
 * @param <K> the type of join key
 * @param <M> the type of input messages on this side of the join
 * @param <OM> the type of input message on the other side of the join
 * @param <JM> the type of join result
 */
class PartialJoinOperatorImpl<K, M, OM, JM> extends OperatorImpl<M, JM> {

  private final JoinOperatorSpec<K, M, OM, JM> joinOpSpec;
  private final boolean isLeftSide; // whether this operator impl is for the left side of the join
  private final PartialJoinFunction<K, M, OM, JM> thisPartialJoinFn;
  private final PartialJoinFunction<K, OM, M, JM> otherPartialJoinFn;
  private final long ttlMs;
  private final Clock clock;

  PartialJoinOperatorImpl(JoinOperatorSpec<K, M, OM, JM> joinOpSpec, boolean isLeftSide,
      PartialJoinFunction<K, M, OM, JM> thisPartialJoinFn,
      PartialJoinFunction<K, OM, M, JM> otherPartialJoinFn,
      Config config, TaskContext context, Clock clock) {
    this.joinOpSpec = joinOpSpec;
    this.isLeftSide = isLeftSide;
    this.thisPartialJoinFn = thisPartialJoinFn;
    this.otherPartialJoinFn = otherPartialJoinFn;
    this.ttlMs = joinOpSpec.getTtlMs();
    this.clock = clock;
  }

  @Override
  protected void handleInit(Config config, OpContext opContext) {
    this.thisPartialJoinFn.init(config, opContext);
  }

  @Override
  public Collection<JM> handleMessage(M message, MessageCollector collector, TaskCoordinator coordinator) {
    try {
      KeyValueStore<K, TimestampedValue<M>> thisState = thisPartialJoinFn.getState();
      KeyValueStore<K, TimestampedValue<OM>> otherState = otherPartialJoinFn.getState();

      K key = thisPartialJoinFn.getKey(message);
      thisState.put(key, new TimestampedValue<>(message, clock.currentTimeMillis()));
      TimestampedValue<OM> otherMessage = otherState.get(key);

      long now = clock.currentTimeMillis();
      if (otherMessage != null && otherMessage.getTimestamp() > now - ttlMs) {
        JM joinResult = thisPartialJoinFn.apply(message, otherMessage.getValue());
        return Collections.singletonList(joinResult);
      }
    } catch (Exception e) {
      throw new SamzaException("Error handling message in PartialJoinOperatorImpl " + getOpImplId(), e);
    }
    return Collections.emptyList();
  }

  @Override
  protected void handleClose() {
    this.thisPartialJoinFn.close();
  }

  protected OperatorSpec<M, JM> getOperatorSpec() {
    return (OperatorSpec<M, JM>) joinOpSpec;
  }

  /**
   * The ID for this {@link PartialJoinOperatorImpl} that includes information about which
   * side of the join it is for.
   *
   * @return the {@link PartialJoinOperatorImpl} ID.
   */
  @Override
  protected String getOpImplId() {
    return isLeftSide ? joinOpSpec.getLeftOpId() : joinOpSpec.getRightOpId();
  }
}
