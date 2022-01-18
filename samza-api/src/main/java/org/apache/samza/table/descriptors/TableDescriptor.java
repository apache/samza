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

import java.util.Map;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.context.TaskContext;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.functions.InitableFunction;
import org.apache.samza.table.Table;
import org.apache.samza.task.InitableTask;


/**
 * A {@link TableDescriptor} can be used for specifying Samza and implementation-specific properties of a {@link Table}.
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
 * For High Level API {@link StreamApplication}s, use {@link StreamApplicationDescriptor#getTable(TableDescriptor)} to
 * obtain the corresponding {@link Table} instance that can be used with the {@link MessageStream} operators like
 * {@link MessageStream#sendTo(Table)}. Alternatively, use {@link TaskContext#getTable(String)} or
 * {@link TaskContext#getUpdatableTable(String)} in {@link InitableFunction#init} to get the table instance for use
 * within operator functions. For Low Level API {@link TaskApplication}s, use {@link TaskContext#getTable(String)} in
 * {@link InitableTask#init} to get the table instance for use within the Task.
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
   * Generate configuration for this table descriptor, the generated configuration
   * should be the complete configuration for this table that can be directly
   * included in the job configuration.
   *
   * Note: although the serdes may have already been set in this instance, their
   * corresponding configuration needs to be generated centrally for consistency
   * and efficiency reasons. Therefore the serde configuration for this table
   * is expected to have already been generated and stored in the {@code jobConfig}.
   *
   * @param jobConfig job configuration
   * @return table configuration
   */
  Map<String, String> toConfig(Config jobConfig);
}