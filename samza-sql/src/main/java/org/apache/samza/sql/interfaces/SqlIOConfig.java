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

package org.apache.samza.sql.interfaces;

import com.google.common.base.Joiner;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang.Validate;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.table.descriptors.CachingTableDescriptor;
import org.apache.samza.table.descriptors.RemoteTableDescriptor;
import org.apache.samza.table.descriptors.TableDescriptor;
import org.apache.samza.system.SystemStream;


/**
 * Configs associated with an IO resource. Both stream and table resources are supported.
 */
public class SqlIOConfig {

  public static final String CFG_SAMZA_REL_CONVERTER = "samzaRelConverterName";
  public static final String CFG_SAMZA_REL_TABLE_KEY_CONVERTER = "samzaRelTableKeyConverterName";
  public static final String CFG_REL_SCHEMA_PROVIDER = "relSchemaProviderName";

  private final String streamId;

  private final String samzaRelConverterName;
  private final String samzaRelTableKeyConverterName;
  private final SystemStream systemStream;

  private final String source;
  private final String relSchemaProviderName;

  private final Config config;

  private final List<String> sourceParts;

  private final Optional<TableDescriptor> tableDescriptor;

  public SqlIOConfig(String systemName, String streamName, Config systemConfig) {
    this(systemName, streamName, Arrays.asList(systemName, streamName), systemConfig, null);
  }

  public SqlIOConfig(String systemName, String streamName, Config systemConfig, TableDescriptor tableDescriptor) {
    this(systemName, streamName, Arrays.asList(systemName, streamName), systemConfig, tableDescriptor);
  }

  public SqlIOConfig(String systemName, String streamName, List<String> sourceParts,
      Config systemConfig, TableDescriptor tableDescriptor) {
    HashMap<String, String> streamConfigs = new HashMap<>(systemConfig);
    this.source = getSourceFromSourceParts(sourceParts);
    this.sourceParts = sourceParts;
    this.systemStream = new SystemStream(systemName, streamName);
    this.tableDescriptor = Optional.ofNullable(tableDescriptor);

    // Remote table has no backing stream associated with it and hence streamId does not make sense. But let's keep it
    // for uniformity. Remote table has table descriptor defined.
    // Local table has both backing stream and a tableDescriptor defined.
    this.streamId = String.format("%s-%s", systemName, streamName);

    samzaRelConverterName = streamConfigs.get(CFG_SAMZA_REL_CONVERTER);
    Validate.notEmpty(samzaRelConverterName, String.format("System %s is not supported. Please check if the system name is provided correctly.", systemName));

    if (isRemoteTable()) {
      samzaRelTableKeyConverterName = streamConfigs.get(CFG_SAMZA_REL_TABLE_KEY_CONVERTER);
      Validate.notEmpty(samzaRelTableKeyConverterName, String.format("System %s is not supported. Please check if the system name is provided correctly.", systemName));
    } else {
      samzaRelTableKeyConverterName = "";
    }

    relSchemaProviderName = streamConfigs.get(CFG_REL_SCHEMA_PROVIDER);

    // Removing the Samza SQL specific configs to get the remaining Samza configs.
    streamConfigs.remove(CFG_SAMZA_REL_CONVERTER);
    streamConfigs.remove(CFG_REL_SCHEMA_PROVIDER);

    if (!isRemoteTable()) {
      // The below config is required for local table and streams but not for remote table.
      streamConfigs.put(String.format(StreamConfig.PHYSICAL_NAME_FOR_STREAM_ID, streamId), streamName);
      if (tableDescriptor != null) {
        // For local table, set the bootstrap config and default offset to oldest
        streamConfigs.put(String.format(StreamConfig.BOOTSTRAP_FOR_STREAM_ID, streamId), "true");
        streamConfigs.put(String.format(StreamConfig.CONSUMER_OFFSET_DEFAULT_FOR_STREAM_ID, streamId), "oldest");
      }
    }

    config = new MapConfig(streamConfigs);
  }

  public static String getSourceFromSourceParts(List<String> sourceParts) {
    return Joiner.on(".").join(sourceParts);
  }

  public List<String> getSourceParts() {
    return sourceParts;
  }

  public String getSystemName() {
    return systemStream.getSystem();
  }

  public String getStreamId() {
    return streamId;
  }

  public String getSamzaRelConverterName() {
    return samzaRelConverterName;
  }

  public String getSamzaRelTableKeyConverterName() {
    return samzaRelTableKeyConverterName;
  }

  public String getRelSchemaProviderName() {
    return relSchemaProviderName;
  }

  public SystemStream getSystemStream() {
    return systemStream;
  }

  public Config getConfig() {
    return config;
  }

  public String getSource() {
    return source;
  }

  public Optional<TableDescriptor> getTableDescriptor() {
    return tableDescriptor;
  }

  public boolean isRemoteTable() {
    return tableDescriptor.isPresent() && (tableDescriptor.get() instanceof RemoteTableDescriptor ||
        tableDescriptor.get() instanceof CachingTableDescriptor);
  }
}
