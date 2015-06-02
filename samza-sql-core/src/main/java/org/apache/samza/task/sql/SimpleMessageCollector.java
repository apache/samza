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

package org.apache.samza.task.sql;

import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.OperatorCallback;
import org.apache.samza.sql.operators.factory.NoopOperatorCallback;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;


public class SimpleMessageCollector implements MessageCollector {
  protected final MessageCollector collector;
  protected final TaskCoordinator coordinator;
  protected OperatorCallback callback;

  /**
   * Ctor that creates the {@code SimpleMessageCollector} from scratch
   * @param collector The {@link org.apache.samza.task.MessageCollector} in the context
   * @param coordinator The {@link org.apache.samza.task.TaskCoordinator} in the context
   * @param callback The {@link org.apache.samza.sql.api.operators.OperatorCallback} in the context
   */
  public SimpleMessageCollector(MessageCollector collector, TaskCoordinator coordinator, OperatorCallback callback) {
    this.collector = collector;
    this.coordinator = coordinator;
    if (callback == null) {
      this.callback = new NoopOperatorCallback();
    } else {
      this.callback = callback;
    }
  }

  /**
   * Ctor that creates the {@code SimpleMessageCollector} from scratch
   * @param collector The {@link org.apache.samza.task.MessageCollector} in the context
   * @param coordinator The {@link org.apache.samza.task.TaskCoordinator} in the context
   */
  public SimpleMessageCollector(MessageCollector collector, TaskCoordinator coordinator) {
    this.collector = collector;
    this.coordinator = coordinator;
  }

  /**
   * This method swaps the {@code callback} with the new one
   *
   * <p> This method allows the {@link org.apache.samza.sql.api.operators.SimpleOperator} to be swapped when the collector
   * is passed down into the next operator's context. Hence, under the new operator's context, the correct {@link org.apache.samza.sql.api.operators.OperatorCallback#afterProcess(Relation, MessageCollector, TaskCoordinator)},
   * and {@link org.apache.samza.sql.api.operators.OperatorCallback#afterProcess(Tuple, MessageCollector, TaskCoordinator)} can be invoked
   *
   * @param callback The new {@link org.apache.samza.sql.api.operators.OperatorCallback} to be set
   */
  public void switchOperatorCallback(OperatorCallback callback) {
    this.callback = callback;
  }

  /**
   * Method is declared to be final s.t. we enforce that the callback functions are called first
   */
  final public void send(Relation deltaRelation) throws Exception {
    Relation rel = this.callback.afterProcess(deltaRelation, collector, coordinator);
    if (rel == null) {
      return;
    }
    this.realSend(rel);
  }

  /**
   * Method is declared to be final s.t. we enforce that the callback functions are called first
   */
  final public void send(Tuple tuple) throws Exception {
    Tuple otuple = this.callback.afterProcess(tuple, collector, coordinator);
    if (otuple == null) {
      return;
    }
    this.realSend(otuple);
  }

  protected void realSend(Relation rel) throws Exception {
    for (KeyValueIterator<?, Tuple> iter = rel.all(); iter.hasNext();) {
      Entry<?, Tuple> entry = iter.next();
      this.collector.send((OutgoingMessageEnvelope) entry.getValue().getMessage());
    }
  }

  protected void realSend(Tuple tuple) throws Exception {
    this.collector.send((OutgoingMessageEnvelope) tuple.getMessage());
  }

  @Override
  public void send(OutgoingMessageEnvelope envelope) {
    this.collector.send(envelope);
  }
}
