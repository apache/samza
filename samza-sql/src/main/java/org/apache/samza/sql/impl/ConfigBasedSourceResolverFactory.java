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
import org.apache.samza.sql.interfaces.SourceResolver;
import org.apache.samza.sql.interfaces.SourceResolverFactory;
import org.apache.samza.sql.interfaces.SqlSystemStreamConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Source Resolver implementation that uses static config to return a config corresponding to a system stream.
 * This Source resolver implementation supports sources of type {systemName}.{streamName}[.$table]
 */
public class ConfigBasedSourceResolverFactory implements SourceResolverFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigBasedSourceResolverFactory.class);

  public static final String CFG_FMT_SAMZA_PREFIX = "systems.%s.";

  @Override
  public SourceResolver create(Config config) {
    return new ConfigBasedSourceResolver(config);
  }

  private class ConfigBasedSourceResolver implements SourceResolver {
    private final String SAMZA_SQL_QUERY_TABLE_KEYWORD = "$table";
    private final Config config;

    public ConfigBasedSourceResolver(Config config) {
      this.config = config;
    }

    @Override
    public SqlSystemStreamConfig fetchSourceInfo(String source) {
      String[] sourceComponents = source.split("\\.");
      boolean isTable = false;
      int systemIdx = 0;
      int endIdx = sourceComponents.length - 1;
      int streamIdx = endIdx;

      // This source resolver expects sources of format {systemName}.{streamName}[.$table]
      if (sourceComponents.length != 2) {
        if (sourceComponents.length != 3 ||
            !sourceComponents[endIdx].toLowerCase().equals(SAMZA_SQL_QUERY_TABLE_KEYWORD)) {
          String msg = String.format("Source %s is not of the format {systemName}.{streamName}[.%s]", source,
              SAMZA_SQL_QUERY_TABLE_KEYWORD);
          LOG.error(msg);
          throw new SamzaException(msg);
        }
      }

      if (sourceComponents[endIdx].toLowerCase().equals(SAMZA_SQL_QUERY_TABLE_KEYWORD)) {
        isTable = true;
        streamIdx = endIdx - 1;
      }

      String systemName = sourceComponents[systemIdx];
      String streamName = sourceComponents[streamIdx];

      return new SqlSystemStreamConfig(systemName, streamName, fetchSystemConfigs(systemName), isTable);
    }

    @Override
    public boolean isTable(String sourceName) {
      String[] sourceComponents = sourceName.split("\\.");
      return sourceComponents[sourceComponents.length - 1].toLowerCase().equals(SAMZA_SQL_QUERY_TABLE_KEYWORD);
    }

    private Config fetchSystemConfigs(String systemName) {
      return config.subset(systemName + ".");
    }
  }
}
