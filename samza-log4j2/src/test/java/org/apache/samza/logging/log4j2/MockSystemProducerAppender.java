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

package org.apache.samza.logging.log4j2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;


/**
 * a mock class which overrides the getConfig method in SystemProducerAppender
 * for testing purpose. Because the environment variable where the config
 * stays is difficult to test.
 */
@Plugin(name = "MockSystemProducer", category = "Core", elementType = "appender", printObject = true)
class MockSystemProducerAppender extends StreamAppender {
  private static Config config;

  protected MockSystemProducerAppender(String name, Filter filter, Layout<? extends Serializable> layout, boolean ignoreExceptions, Config config) {
    super(name, filter, layout, ignoreExceptions);
  }

  @PluginFactory
  public static MockSystemProducerAppender createAppender(
      @PluginAttribute("name") final String name,
      @PluginElement("Filter") final Filter filter,
      @PluginElement("Layout") Layout<? extends Serializable> layout,
      @PluginAttribute(value = "ignoreExceptions", defaultBoolean = true) final boolean ignoreExceptions,
      @PluginElement("Config") final Config testConfig) {
    if (testConfig == null) {
      initConfig();
    } else {
      config = testConfig;
    }
    return new MockSystemProducerAppender(name, filter, layout, ignoreExceptions, config);
  }

  @Override
  protected Config getConfig() {
    return config;
  }

  private static void initConfig() {
    Map<String, String> map = new HashMap<String, String>();
    map.put("job.name", "log4jTest");
    map.put("systems.mock.samza.factory", MockSystemFactory.class.getCanonicalName());
    map.put("task.log4j.system", "mock");
    config = new MapConfig(map);
  }
}

