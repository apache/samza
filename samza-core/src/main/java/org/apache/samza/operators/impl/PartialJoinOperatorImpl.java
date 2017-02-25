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
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.functions.PartialJoinFunction;
import org.apache.samza.operators.functions.PartialJoinFunction.PartialJoinMessage;
import org.apache.samza.operators.spec.PartialJoinOperatorSpec;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of a {@link PartialJoinOperatorSpec} that joins messages of type {@code M} in this stream
 * with buffered messages of type {@code JM} in the other stream.
 *
 * @param <M>  type of messages in the input stream
 * @param <JM>  type of messages in the stream to join with
 * @param <RM>  type of messages in the joined stream
 */
class PartialJoinOperatorImpl<K, M, JM, RM> extends OperatorImpl<M, RM> {

  private final PartialJoinFunction<K, M, JM, RM> thisPartialJoinFn;
  private final PartialJoinFunction<K, JM, M, RM> otherPartialJoinFn;
  private final long ttlMs;

  PartialJoinOperatorImpl(PartialJoinOperatorSpec<K, M, JM, RM> partialJoinOperatorSpec, MessageStreamImpl<M> source,
      Config config, TaskContext context) {
    this.thisPartialJoinFn = partialJoinOperatorSpec.getThisPartialJoinFn();
    this.otherPartialJoinFn = partialJoinOperatorSpec.getOtherPartialJoinFn();
    this.ttlMs = partialJoinOperatorSpec.getTtlMs();
  }

  @Override
  public void onNext(M message, MessageCollector collector, TaskCoordinator coordinator) {
    K key = thisPartialJoinFn.getKey(message);
    thisPartialJoinFn.getState().put(key, new PartialJoinMessage<>(message, System.currentTimeMillis()));
    PartialJoinMessage<JM> otherMessage = otherPartialJoinFn.getState().get(key);
    if (otherMessage != null) {
      RM joinResult = thisPartialJoinFn.apply(message, otherMessage.getMessage());
      this.propagateResult(joinResult, collector, coordinator);
    }
  }

  @Override
  public void onTimer(MessageCollector collector, TaskCoordinator coordinator) {
    KeyValueStore<K, PartialJoinMessage<M>> thisState = thisPartialJoinFn.getState();
    KeyValueIterator<K, PartialJoinMessage<M>> iterator = thisState.all();
    List<K> keysToRemove = new ArrayList<>();
    long now = System.currentTimeMillis();

    while (iterator.hasNext()) {
      Entry<K, PartialJoinMessage<M>> entry = iterator.next();
      if (entry.getValue().getTimestamp() < now - ttlMs) {
        keysToRemove.add(entry.getKey());
      } else {
        break;
      }
    }

    thisState.deleteAll(keysToRemove);

    this.propagateTimer(collector, coordinator);
  }

}
