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
import org.apache.samza.sql.interfaces.SqlSystemStreamConfig;


public class TestSourceResolverFactory implements SourceResolverFactory {
  @Override
  public SourceResolver create(Config config) {
    return new TestSourceResolver(config);
  }

  private class TestSourceResolver implements SourceResolver {
    private final Config config;

    public TestSourceResolver(Config config) {
      this.config = config;
    }

    @Override
    public SqlSystemStreamConfig fetchSourceInfo(String sourceName) {
      String[] sourceComponents = sourceName.split("\\.");
      Config systemConfigs = config.subset(sourceComponents[0] + ".");
      return new SqlSystemStreamConfig(sourceComponents[0], sourceComponents[sourceComponents.length - 1],
          Arrays.asList(sourceComponents), systemConfigs);
    }
  }
}
