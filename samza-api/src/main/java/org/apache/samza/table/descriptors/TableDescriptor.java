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
package org.apache.samza.table.descriptors;

import org.apache.samza.annotation.InterfaceStability;


/**
 * A {@link TableDescriptor} can be used for specifying Samza and implementation-specific properties of a
 * {@link org.apache.samza.table.Table}.
 * <p>
 * Table properties provided in configuration override corresponding properties specified using a descriptor.
 * <p>
 * This is the base descriptor for a table. Use a implementation-specific descriptor (e.g. RocksDBTableDescriptor) to
 * use it in the application. For example:
 * <pre>{@code
 * RocksDbTableDescriptor tableDescriptor = new RocksDbTableDescriptor("table",
 *         KVSerde.of(new IntegerSerde(), new StringSerde("UTF-8")))
 *     .withBlockSize(1024)
 *     .withConfig("some-key", "some-value");
 * }
 * </pre>
 * For High Level API {@link org.apache.samza.application.StreamApplication}s, use
 * {@link org.apache.samza.application.descriptors.StreamApplicationDescriptor#getTable(TableDescriptor)} to obtain
 * the corresponding {@link org.apache.samza.table.Table} instance that can be used with the
 * {@link org.apache.samza.operators.MessageStream} operators like
 * {@link org.apache.samza.operators.MessageStream#sendTo(org.apache.samza.table.Table)}.
 * Alternatively, use {@link org.apache.samza.context.TaskContext#getTable(String)} in
 * {@link org.apache.samza.operators.functions.InitableFunction#init} to get the table instance for use within
 * operator functions.
 * For Low Level API {@link org.apache.samza.application.TaskApplication}s, use
 * {@link org.apache.samza.context.TaskContext#getTable(String)} in
 * {@link org.apache.samza.task.InitableTask#init} to get the table instance for use within the Task.
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 * @param <D> the type of the concrete table descriptor
 */
@InterfaceStability.Unstable
public interface TableDescriptor<K, V, D extends TableDescriptor<K, V, D>> {

  /**
   * Get the id of the table
   * @return id of the table
   */
  String getTableId();

  /**
   * Add a configuration entry for the table
   *
   * @param key the key
   * @param value the value
   * @return this table descriptor instance
   */
  D withConfig(String key, String value);

}