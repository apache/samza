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
import org.apache.samza.serializers.KVSerde;
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

  static final public String INTERNAL_ENABLE_CHANGELOG = "internal.enable.changelog";
  static final public String INTERNAL_CHANGELOG_STREAM = "internal.changelog.stream";
  static final public String INTERNAL_CHANGELOG_REPLICATION_FACTOR = "internal.changelog.replication.factor";

  protected List<String> sideInputs;
  protected SideInputsProcessor sideInputsProcessor;
  protected boolean enableChangelog;
  protected String changelogStream;
  protected Integer changelogReplicationFactor;

  /**
   * Constructs a table descriptor instance
   * @param tableId Id of the table, it must confirm to pattern { @literal [\\d\\w-_]+ }
   */
  public BaseLocalStoreBackedTableDescriptor(String tableId) {
    super(tableId);
  }

  /**
   * Constructs a table descriptor instance
   * @param tableId Id of the table, it must confirm to pattern { @literal [\\d\\w-_]+ }
   * @param serde the serde for key and value
   */
  public BaseLocalStoreBackedTableDescriptor(String tableId, KVSerde<K, V> serde) {
    super(tableId, serde);
  }

  public D withSideInputs(List<String> sideInputs) {
    this.sideInputs = sideInputs;
    // Disable changelog
    this.enableChangelog = false;
    this.changelogStream = null;
    this.changelogReplicationFactor = null;
    return (D) this;
  }

  public D withSideInputsProcessor(SideInputsProcessor sideInputsProcessor) {
    this.sideInputsProcessor = sideInputsProcessor;
    return (D) this;
  }

  /**
   * Enable changelog for this table, by default changelog is disabled. When the
   * changelog stream name is not specified, it is automatically generated in
   * the format { @literal [job-name]-[job-id]-table-[table-id] }.
   * Refer to <code>stores.store-name.changelog</code> in Samza configuration guide
   *
   * @return this table descriptor instance
   */
  public D withChangelogEnabled() {
    this.enableChangelog = true;
    return (D) this;
  }

  /**
   * Refer to <code>stores.store-name.changelog</code> in Samza configuration guide
   *
   * @param changelogStream changelog stream name
   * @return this table descriptor instance
   */
  public D withChangelogStream(String changelogStream) {
    this.enableChangelog = true;
    this.changelogStream = changelogStream;
    return (D) this;
  }

  /**
   * Refer to <code>stores.store-name.changelog.replication.factor</code> in Samza configuration guide
   *
   * @param replicationFactor replication factor
   * @return this table descriptor instance
   */
  public D withChangelogReplicationFactor(int replicationFactor) {
    this.enableChangelog = true;
    this.changelogReplicationFactor = replicationFactor;
    return (D) this;
  }

  @Override
  protected void generateTableSpecConfig(Map<String, String> tableSpecConfig) {
    super.generateTableSpecConfig(tableSpecConfig);

    tableSpecConfig.put(INTERNAL_ENABLE_CHANGELOG, String.valueOf(enableChangelog));
    if (enableChangelog) {
      if (changelogStream != null) {
        tableSpecConfig.put(INTERNAL_CHANGELOG_STREAM, changelogStream);
      }
      if (changelogReplicationFactor != null) {
        tableSpecConfig.put(INTERNAL_CHANGELOG_REPLICATION_FACTOR, String.valueOf(changelogReplicationFactor));
      }
    }
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
    if (!enableChangelog) {
      Preconditions.checkState(changelogStream == null,
          String.format("Invalid changelog configuration for table: %s. Changelog " +
              "must be enabled, when changelog stream name is provided", tableId));
      Preconditions.checkState(changelogReplicationFactor == null,
          String.format("Invalid changelog configuration for table: %s. Changelog " +
              "must be enabled, when changelog replication factor is provided", tableId));
    }
  }

}
