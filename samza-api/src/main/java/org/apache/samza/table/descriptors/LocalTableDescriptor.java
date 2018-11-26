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

import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.storage.SideInputsProcessor;
import org.apache.samza.table.utils.SerdeUtils;

/**
 * Table descriptor for store backed tables.
 *
 * @param <K> the type of the key in this table
 * @param <V> the type of the value in this table
 * @param <D> the type of the concrete table descriptor
 */
abstract public class LocalTableDescriptor<K, V, D extends LocalTableDescriptor<K, V, D>>
    extends BaseTableDescriptor<K, V, D> {

  public static final Pattern SYSTEM_STREAM_NAME_PATTERN = Pattern.compile("[\\d\\w-_.]+");

  // Serdes for this table
  protected final KVSerde<K, V> serde;

  // changelog parameters
  protected boolean enableChangelog; // Disabled by default
  protected String changelogStream;
  protected Integer changelogReplicationFactor;

  // Side input parameters
  protected List<String> sideInputs;
  protected SideInputsProcessor sideInputsProcessor;

  /**
   * Constructs a table descriptor instance
   * @param tableId Id of the table, it must conform to pattern {@literal [\\d\\w-_]+}
   * @param serde the serde for key and value
   */
  public LocalTableDescriptor(String tableId, KVSerde<K, V> serde) {
    super(tableId);
    this.serde = serde;
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

  /**
   * {@inheritDoc}
   *
   * Note: Serdes are expected to be generated during configuration generation
   * of the job node, which is not handled here.
   */
  @Override
  public Map<String, String> toConfig(Config jobConfig) {

    Map<String, String> tableConfig = new HashMap<>(super.toConfig(jobConfig));

    JavaTableConfig javaTableConfig = new JavaTableConfig(jobConfig);

    if (sideInputs != null && !sideInputs.isEmpty()) {
      sideInputs.forEach(si -> Preconditions.checkState(isValidSystemStreamName(si), String.format(
          "Side input stream %s doesn't confirm to pattern %s", si, SYSTEM_STREAM_NAME_PATTERN)));
      String formattedSideInputs = String.join(",", sideInputs);
      addStoreConfig("side.inputs", formattedSideInputs, tableConfig);
      addStoreConfig("side.inputs.processor.serialized.instance",
          SerdeUtils.serialize("Side Inputs Processor", sideInputsProcessor), tableConfig);
    }

    // Changelog configuration
    if (enableChangelog) {
      if (StringUtils.isEmpty(changelogStream)) {
        String jobName = jobConfig.get("job.name");
        Preconditions.checkNotNull(jobName, "job.name not found in job config");
        String jobId = jobConfig.get("job.id");
        Preconditions.checkNotNull(jobId, "job.id not found in job config");
        changelogStream = String.format("%s-%s-table-%s", jobName, jobId, tableId);
      }

      Preconditions.checkState(isValidSystemStreamName(changelogStream), String.format(
          "Changelog stream %s doesn't confirm to pattern %s", changelogStream, SYSTEM_STREAM_NAME_PATTERN));
      addStoreConfig("changelog", changelogStream, tableConfig);

      if (changelogReplicationFactor != null) {
        addStoreConfig("changelog.replication.factor", changelogReplicationFactor.toString(), tableConfig);
      }
    }

    return Collections.unmodifiableMap(tableConfig);
  }

  /**
   * Get side input stream names
   * @return side inputs
   */
  public List<String> getSideInputs() {
    return sideInputs;
  }

  /**
   * Get the serde assigned to this {@link TableDescriptor}
   *
   * @return {@link KVSerde} used by this table
   */
  public KVSerde<K, V> getSerde() {
    return serde;
  }

  @Override
  protected void validate() {
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

  /**
   * Helper method to add a store level config item to table configuration
   * @param key key of the config item
   * @param value value of the config item
   * @param tableConfig table configuration
   */
  protected void addStoreConfig(String key, String value, Map<String, String> tableConfig) {
    tableConfig.put(String.format("stores.%s.%s", tableId, key), value);
  }

  private boolean isValidSystemStreamName(String name) {
    return StringUtils.isNotBlank(name) && SYSTEM_STREAM_NAME_PATTERN.matcher(name).matches();
  }

}
