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

package org.apache.samza.sql.impl;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.sql.serializers.SamzaSqlRelMessageSerdeFactory;
import org.apache.samza.sql.serializers.SamzaSqlRelRecordSerdeFactory;
import org.apache.samza.table.descriptors.TableDescriptor;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.sql.interfaces.SqlIOResolver;
import org.apache.samza.sql.interfaces.SqlIOResolverFactory;
import org.apache.samza.sql.interfaces.SqlIOConfig;
import org.apache.samza.storage.kv.descriptors.RocksDbTableDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.sql.runner.SamzaSqlApplicationConfig.CFG_METADATA_TOPIC_PREFIX;
import static org.apache.samza.sql.runner.SamzaSqlApplicationConfig.DEFAULT_METADATA_TOPIC_PREFIX;


/**
 * Source Resolver implementation that uses static config to return a config corresponding to a system stream.
 * This Source resolver implementation supports sources of type {systemName}.{streamName}[.$table]
 * {systemName}.{streamName} indicates a stream
 * {systemName}.{streamName}.$table indicates a table
 */
public class ConfigBasedIOResolverFactory implements SqlIOResolverFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigBasedIOResolverFactory.class);

  public static final String CFG_FMT_SAMZA_PREFIX = "systems.%s.";

  @Override
  public SqlIOResolver create(Config resolverConfig, Config fullConfig) {
    return new ConfigBasedIOResolver(resolverConfig);
  }

  private class ConfigBasedIOResolver implements SqlIOResolver {
    private static final String SAMZA_SQL_QUERY_TABLE_KEYWORD = "$table";
    private final Config config;
    private final String changeLogStorePrefix;

    public ConfigBasedIOResolver(Config config) {
      this.config = config;
      String metadataTopicPrefix = config.get(CFG_METADATA_TOPIC_PREFIX, DEFAULT_METADATA_TOPIC_PREFIX);
      this.changeLogStorePrefix = metadataTopicPrefix + (metadataTopicPrefix.isEmpty() ? "" : "_");
    }

    @Override
    public SqlIOConfig fetchSourceInfo(String source) {
      return fetchSystemInfo(source);
    }

    private SqlIOConfig fetchSystemInfo(String name) {
      String[] sourceComponents = name.split("\\.");
      boolean isTable = isTable(sourceComponents);

      // This source resolver expects sources of format {systemName}.{streamName}[.$table]
      //  * First source part is always system name.
      //  * The last source part could be either a "$table" keyword or stream name. If it is "$table", then stream name
      //    should be the one before the last source part.
      int endIdx = sourceComponents.length - 1;
      int streamIdx = isTable ? endIdx - 1 : endIdx;
      boolean invalidQuery = false;

      if (sourceComponents.length != 2) {
        if (sourceComponents.length != 3 ||
            !sourceComponents[endIdx].equalsIgnoreCase(SAMZA_SQL_QUERY_TABLE_KEYWORD)) {
          invalidQuery = true;
        }
      } else {
        if (sourceComponents[0].equalsIgnoreCase(SAMZA_SQL_QUERY_TABLE_KEYWORD) ||
            sourceComponents[1].equalsIgnoreCase(SAMZA_SQL_QUERY_TABLE_KEYWORD)) {
          invalidQuery = true;
        }
      }

      if (invalidQuery) {
        String msg = String.format("Source %s is not of the format {systemName}.{streamName}[.%s]", name,
            SAMZA_SQL_QUERY_TABLE_KEYWORD);
        LOG.error(msg);
        throw new SamzaException(msg);
      }

      String systemName = sourceComponents[0];
      String streamName = sourceComponents[streamIdx];

      TableDescriptor tableDescriptor = null;
      if (isTable) {
        String tableId = changeLogStorePrefix + "InputTable-" + name.replace(".", "-").replace("$", "-");
        SamzaSqlRelRecordSerdeFactory.SamzaSqlRelRecordSerde keySerde =
            (SamzaSqlRelRecordSerdeFactory.SamzaSqlRelRecordSerde) new SamzaSqlRelRecordSerdeFactory().getSerde(null, null);
        SamzaSqlRelMessageSerdeFactory.SamzaSqlRelMessageSerde valueSerde =
            (SamzaSqlRelMessageSerdeFactory.SamzaSqlRelMessageSerde) new SamzaSqlRelMessageSerdeFactory().getSerde(null, null);
        tableDescriptor = new RocksDbTableDescriptor(tableId, KVSerde.of(keySerde, valueSerde))
            .withChangelogEnabled();
      }

      return new SqlIOConfig(systemName, streamName, fetchSystemConfigs(systemName), tableDescriptor);
    }

    @Override
    public SqlIOConfig fetchSinkInfo(String sink) {
      return fetchSystemInfo(sink);
    }

    private boolean isTable(String[] sourceComponents) {
      return sourceComponents[sourceComponents.length - 1].equalsIgnoreCase(SAMZA_SQL_QUERY_TABLE_KEYWORD);
    }

    private Config fetchSystemConfigs(String systemName) {
      return config.subset(systemName + ".");
    }
  }
}
