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
package org.apache.samza.storage.kv.descriptors;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;

import org.apache.samza.table.descriptors.BaseTableDescriptor;
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
   * @param tableId Id of the table, it must conform to pattern {@literal [\\d\\w-_]+}
   */
  public BaseLocalStoreBackedTableDescriptor(String tableId) {
    super(tableId);
  }

  /**
   * Constructs a table descriptor instance
   * @param tableId Id of the table, it must conform to pattern {@literal [\\d\\w-_]+}
   * @param serde the serde for key and value
   */
  public BaseLocalStoreBackedTableDescriptor(String tableId, KVSerde<K, V> serde) {
    super(tableId, serde);
  }

  /**
   * Add side inputs to the table. Each stream is of the format
   * <i>system-name</i>.<i>stream-name</i>. The streams are marked as bootstrap streams and once the table is bootstrapped, it is
   * updated on the background in change capture mode.
   * Applications should specify the transformation logic using {@link #withSideInputsProcessor(SideInputsProcessor)}, which is
   * will be applied on the side input messages before writing to the table.
   *
   * @param sideInputs list of side input streams
   * @return this table descriptor instance
   */
  public D withSideInputs(List<String> sideInputs) {
    this.sideInputs = sideInputs;
    // Disable changelog
    this.enableChangelog = false;
    this.changelogStream = null;
    this.changelogReplicationFactor = null;
    return (D) this;
  }

  /**
   * Provide the {@link SideInputsProcessor} for this table. It is a mapper function applied on the side inputs
   * specified using {@link #withSideInputs(List)} prior to writing the entries to the table.
   *
   * @param sideInputsProcessor a side input processor
   * @return this table descriptor instance
   */
  public D withSideInputsProcessor(SideInputsProcessor sideInputsProcessor) {
    this.sideInputsProcessor = sideInputsProcessor;
    return (D) this;
  }

  /**
   * Enable changelog for this table, by default changelog is disabled. When the
   * changelog stream name is not specified, it is automatically generated in
   * the format {@literal [job-name]-[job-id]-table-[table-id]}.
   * Refer to <code>stores.store-name.changelog</code> in Samza configuration guide
   *
   * @return this table descriptor instance
   */
  public D withChangelogEnabled() {
    this.enableChangelog = true;
    return (D) this;
  }

  /**
   * Samza stores are local to a container. If the container fails, the contents of
   * the store are lost. To prevent loss of data, you need to set this property to
   * configure a changelog stream: Samza then ensures that writes to the store are
   * replicated to this stream, and the store is restored from this stream after a
   * failure. The value of this property is given in the form system-name.stream-name.
   * The "system-name" part is optional. If it is omitted you must specify the system
   * in <code>job.changelog.system</code> config. Any output stream can be used as
   * changelog, but you must ensure that only one job ever writes to a given changelog
   * stream (each instance of a job and each store needs its own changelog stream).
   * <p>
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
   * The property defines the number of replicas to use for the change log stream.
   * <p>
   * Default value is <code>stores.default.changelog.replication.factor</code>.
   * <p>
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
