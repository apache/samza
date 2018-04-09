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

package org.apache.samza.sql.testutil;

import java.util.Arrays;
import org.apache.samza.config.Config;
import org.apache.samza.sql.interfaces.SourceResolver;
import org.apache.samza.sql.interfaces.SourceResolverFactory;
import org.apache.samza.sql.interfaces.SqlSystemSourceConfig;


public class TestSourceResolverFactory implements SourceResolverFactory {
  @Override
  public SourceResolver create(Config config) {
    return new TestSourceResolver(config);
  }

  private class TestSourceResolver implements SourceResolver {
    private final String SAMZA_SQL_QUERY_TABLE_KEYWORD = "$table";
    private final Config config;

    public TestSourceResolver(Config config) {
      this.config = config;
    }

    @Override
    public SqlSystemSourceConfig fetchSourceInfo(String sourceName) {
      String[] sourceComponents = sourceName.split("\\.");
      boolean isTable = false;
      int systemIdx = 0;
      int endIdx = sourceComponents.length - 1;
      int streamIdx = endIdx;

      if (sourceComponents[endIdx].equalsIgnoreCase(SAMZA_SQL_QUERY_TABLE_KEYWORD)) {
        isTable = true;
        streamIdx = endIdx - 1;
      }
      Config systemConfigs = config.subset(sourceComponents[systemIdx] + ".");
      return new SqlSystemSourceConfig(sourceComponents[systemIdx], sourceComponents[streamIdx],
          Arrays.asList(sourceComponents), systemConfigs, isTable);
    }

    @Override
    public boolean isTable(String sourceName) {
      String[] sourceComponents = sourceName.split("\\.");
      return sourceComponents[sourceComponents.length - 1].equalsIgnoreCase(SAMZA_SQL_QUERY_TABLE_KEYWORD);
    }
  }
}
