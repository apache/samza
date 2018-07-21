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
package org.apache.samza.operators;

import java.util.List;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.storage.SideInputProcessor;


/**
 * User facing class to collect metadata that fully describes a
 * Samza table. This interface should be implemented by concrete table implementations.
 * <p>
 * Typical user code should look like the following, notice <code>withConfig()</code>
 * is defined in this class and the rest in subclasses.
 *
 * <pre>
 * {@code
 * TableDescriptor<Integer, String, ?> tableDesc = new RocksDbTableDescriptor("tbl")
 *     .withSerde(KVSerde.of(new IntegerSerde(), new StringSerde("UTF-8")))
 *     .withBlockSize(1024)
 *     .withConfig("some-key", "some-value");
 * }
 * </pre>

 * Once constructed, a table descriptor can be registered with the system. Internally,
 * the table descriptor is then converted to a {@link org.apache.samza.table.TableSpec},
 * which is used to track tables internally.
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 * @param <D> the type of the concrete table descriptor
 */
@InterfaceStability.Unstable
public interface TableDescriptor<K, V, D extends TableDescriptor<K, V, D>> {

  /**
   * Get the Id of the table
   * @return Id of the table
   */
  String getTableId();

  /**
   * Set the Serde for this table
   * @param serde the serde
   * @return this table descriptor instance
   * @throws IllegalArgumentException if null is provided
   */
  D withSerde(KVSerde<K, V> serde);

  /**
   * Add a configuration entry for the table
   * @param key the key
   * @param value the value
   * @return this table descriptor instance
   */
  D withConfig(String key, String value);

  /**
   * Add side inputs to this table.
   *
   * @param sideInputs list of side inputs
   *
   * @return this table descriptor instance
   */
  default D withSideInputs(List<String> sideInputs) {
    return (D) this;
  }

  /**
   * Adds a {@link SideInputProcessor} to this table.
   *
   * @param sideInputProcessor side input processor
   *
   * @return this table descriptor instance
   */
  default D withSideInputProcessor(SideInputProcessor sideInputProcessor) {
    return (D) this;
  }
}