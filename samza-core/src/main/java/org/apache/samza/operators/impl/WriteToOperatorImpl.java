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

import java.util.Collection;
import java.util.Collections;

import org.apache.samza.config.Config;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.WriteToOperatorSpec;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;


/**
 * Implementation of a write-stream-to-table operator that first retrieve the key and
 * value for the record by applying key and value extractor, and then store the record
 * in the table.
 *
 * @param <K> the type of the record key
 * @param <V> the type of the record value
 * @param <M> the type of the incoming message
 */
public class WriteToOperatorImpl<K, V, M> extends OperatorImpl<M, Void> {

  private final WriteToOperatorSpec<K, V, M> writeToOpSpec;
  private final ReadWriteTable<K, V> table;

  WriteToOperatorImpl(WriteToOperatorSpec<K, V, M> writeToOpSpec, Config config, TaskContext context) {
    this.writeToOpSpec = writeToOpSpec;
    this.table = (ReadWriteTable) context.getTable(writeToOpSpec.getTableSpec().getId());
  }

  @Override
  protected void handleInit(Config config, TaskContext context) {
  }

  @Override
  protected Collection<Void> handleMessage(M message, MessageCollector collector, TaskCoordinator coordinator) {
    K key = writeToOpSpec.getKeyExtractor().apply(message);
    V value = writeToOpSpec.getValueExtractor().apply(message);
    table.put(key, value);
    // there should be no further chained operators since this is a terminal operator.
    return Collections.emptyList();
  }

  @Override
  protected void handleClose() {
    table.close();
  }

  @Override
  protected OperatorSpec<M, Void> getOperatorSpec() {
    return writeToOpSpec;
  }
}
