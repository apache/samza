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

package org.apache.samza.table.remote;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.apache.samza.SamzaException;
import org.apache.samza.operators.BaseTableDescriptor;
import org.apache.samza.table.TableSpec;

import com.google.common.base.Preconditions;


/**
 * Table descriptor for remote store backed tables
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public class RemoteTableDescriptor<K, V> extends BaseTableDescriptor<K, V, RemoteTableDescriptor<K, V>> {
  // Input support for a specific remote store (required)
  private TableReadFunction<K, V> readFn;

  // Output support for a specific remote store (optional)
  private TableWriteFunction<K, V> writeFn;

  /**
   * Construct a table descriptor instance
   * @param tableId Id of the table
   */
  public RemoteTableDescriptor(String tableId) {
    super(tableId);
  }

  @Override
  public TableSpec getTableSpec() {
    validate();

    Map<String, String> tableSpecConfig = new HashMap<>();
    generateTableSpecConfig(tableSpecConfig);

    // Serialize and store reader/writer functions with config
    tableSpecConfig.put(RemoteReadableTable.READ_FN, serializeObject("read function", readFn));

    if (writeFn != null) {
      tableSpecConfig.put(RemoteReadableTable.WRITE_FN, serializeObject("write function", writeFn));
    }

    return new TableSpec(tableId, serde, RemoteTableProviderFactory.class.getName(), tableSpecConfig);
  }

  /**
   * Use specified TableReadFunction with remote table.
   * @param readFn read function instance
   * @return this table descriptor instance
   */
  public RemoteTableDescriptor<K, V> withReadFunction(TableReadFunction<K, V> readFn) {
    Preconditions.checkNotNull(readFn, "null read function");
    this.readFn = readFn;
    return this;
  }

  /**
   * Use specified TableWriteFunction with remote table.
   * @param writeFn write function instance
   * @return this table descriptor instance
   */
  public RemoteTableDescriptor<K, V> withWriteFunction(TableWriteFunction<K, V> writeFn) {
    Preconditions.checkNotNull(writeFn, "null write function");
    this.writeFn = writeFn;
    return this;
  }

  /**
   * Helper method to serialize Java objects as Base64 strings
   * @param name name of the object (for error reporting)
   * @param object object to be serialized
   * @return Base64 representation of the object
   */
  private String serializeObject(String name, Object object) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(object);
      return Base64.getEncoder().encodeToString(baos.toByteArray());
    } catch (IOException e) {
      throw new SamzaException("Failed to serialize " + name, e);
    }
  }

  @Override
  protected void validate() {
    super.validate();
    Preconditions.checkNotNull(readFn, "TableReadFunction is required.");
  }
}
