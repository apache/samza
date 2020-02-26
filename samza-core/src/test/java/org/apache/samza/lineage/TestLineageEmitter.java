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
package org.apache.samza.lineage;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.LineageConfig;
import org.apache.samza.config.MapConfig;
import org.junit.Test;


public class TestLineageEmitter {

  @Test(expected = ConfigException.class)
  public void testEmitWhenNotConfigLineageFactory() {
    Map<String, String> configs = new HashMap<>();
    configs.put(LineageConfig.LINEAGE_REPORTER_FACTORY, "org.apache.samza.lineage.mock.MockLineageReporterFactory");
    LineageEmitter.emit(new MapConfig(configs));
  }

  @Test(expected = ConfigException.class)
  public void testEmitWhenNotConfigLineageReporterFactory() {
    Map<String, String> configs = new HashMap<>();
    configs.put(LineageConfig.LINEAGE_FACTORY, "org.apache.samza.lineage.mock.MockLineageFactory");
    LineageEmitter.emit(new MapConfig(configs));
  }

  @Test
  public void testEmitWhenConfigNeitherOfLineageFactoryAndLineageRepoterFactory() {
    Map<String, String> configs = new HashMap<>();
    LineageEmitter.emit(new MapConfig(configs));
  }

  @Test
  public void testEmit() {
    Map<String, String> configs = new HashMap<>();
    configs.put(LineageConfig.LINEAGE_FACTORY, "org.apache.samza.lineage.mock.MockLineageFactory");
    configs.put(LineageConfig.LINEAGE_REPORTER_FACTORY, "org.apache.samza.lineage.mock.MockLineageReporterFactory");
    LineageEmitter.emit(new MapConfig(configs));
  }

}
