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
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.system.SystemStream;


/**
 * Configs associated with an IO resource. Both stream and table resources are supported.
 */
public class SqlIOConfig {

  public static final String CFG_SAMZA_REL_CONVERTER = "samzaRelConverterName";
  public static final String CFG_REL_SCHEMA_PROVIDER = "relSchemaProviderName";

  private final String systemName;

  private final String streamName;

  private final String samzaRelConverterName;
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
    this.systemName = systemName;
    this.streamName = streamName;
    this.source = getSourceFromSourceParts(sourceParts);
    this.sourceParts = sourceParts;
    this.systemStream = new SystemStream(systemName, streamName);
    this.tableDescriptor = Optional.ofNullable(tableDescriptor);

    samzaRelConverterName = streamConfigs.get(CFG_SAMZA_REL_CONVERTER);
    Validate.notEmpty(samzaRelConverterName,
        String.format("%s is not set or empty for system %s", CFG_SAMZA_REL_CONVERTER, systemName));

    relSchemaProviderName = streamConfigs.get(CFG_REL_SCHEMA_PROVIDER);

    // Removing the Samza SQL specific configs to get the remaining Samza configs.
    streamConfigs.remove(CFG_SAMZA_REL_CONVERTER);
    streamConfigs.remove(CFG_REL_SCHEMA_PROVIDER);

    // Currently, only local table is supported. And it is assumed that all tables are local tables.
    if (tableDescriptor != null) {
      streamConfigs.put(String.format(StreamConfig.BOOTSTRAP_FOR_STREAM_ID(), streamName), "true");
      streamConfigs.put(String.format(StreamConfig.CONSUMER_OFFSET_DEFAULT_FOR_STREAM_ID(), streamName), "oldest");
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
    return systemName;
  }

  public String getStreamName() {
    return streamName;
  }

  public String getSamzaRelConverterName() {
    return samzaRelConverterName;
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
}
