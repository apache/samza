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
package org.apache.samza.storage.kv;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;

import org.apache.samza.operators.BaseTableDescriptor;
import org.apache.samza.storage.SideInputsProcessor;


/**
 * Table descriptor for store backed tables.
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 * @param <D> the type of the concrete table descriptor
 */
abstract public class BaseLocalStoreBackedTableDescriptor<K, V, D extends BaseLocalStoreBackedTableDescriptor<K, V, D>>
    extends BaseTableDescriptor<K, V, D> {
  protected List<String> sideInputs;
  protected SideInputsProcessor sideInputsProcessor;

  /**
   * Constructs a table descriptor instance
   * @param tableId Id of the table
   */
  public BaseLocalStoreBackedTableDescriptor(String tableId) {
    super(tableId);
  }

  public D withSideInputs(List<String> sideInputs) {
    this.sideInputs = sideInputs;
    return (D) this;
  }

  public D withSideInputsProcessor(SideInputsProcessor sideInputsProcessor) {
    this.sideInputsProcessor = sideInputsProcessor;
    return (D) this;
  }

  @Override
  protected void generateTableSpecConfig(Map<String, String> tableSpecConfig) {
    super.generateTableSpecConfig(tableSpecConfig);
  }

  /**
   * Validate that this table descriptor is constructed properly
   */
  protected void validate() {
    super.validate();
    if (sideInputs != null || sideInputsProcessor != null) {
      Preconditions.checkArgument(sideInputs != null && !sideInputs.isEmpty() && sideInputsProcessor != null,
          String.format("Invalid side input configuration for table: %s. " +
              "Both side inputs and the processor must be provided", tableId));
    }
  }

}
