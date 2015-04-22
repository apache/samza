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

package org.apache.samza.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@link org.apache.samza.config.ConfigRewriter} that takes system environment variables
 * and adds them to the existing {@link org.apache.samza.config.Config}. It will over-ride any
 * existing properties that were set before this rewriter is called.
 *
 * <p>
 * The following mappings are applied to any environment variable that starts with the prefix
 * {@value org.apache.samza.config.EnvironmentConfigRewriter#SAMZA_KEY_PREFIX}.
 * </p>
 *
 * <ul>
 *  <li>The prefixed of {@value org.apache.samza.config.EnvironmentConfigRewriter#SAMZA_KEY_PREFIX} is stripped.</li>
 *  <li>The remaining key is downcased.</li>
 *  <li>All underscores are converted to full stops.</li>
 * </ul>
 *
 *  <p>
 *  For example the environment variable <i>SAMZA_FOO_BAR=baz</i> will be set as
 *  the<i>foo.bar=baz</i> samza property.
 *  </p>
 *
 */
public class EnvironmentConfigRewriter implements ConfigRewriter {
  public static final String SAMZA_KEY_PREFIX = "SAMZA_";
  public static final Logger LOGGER = LoggerFactory.getLogger(EnvironmentConfigRewriter.class);

  @Override
  public Config rewrite(String name, Config config) {
    return rewrite(config, System.getenv());
  }

  protected Config rewrite(Config config, Map<String, String> env) {
    Map<String, String> envConfig = new HashMap<>();

    for (Map.Entry<String, String> entry: env.entrySet()) {
      if (entry.getKey().startsWith(SAMZA_KEY_PREFIX)) {
        String key = renameKey(entry.getKey());
        String value = entry.getValue();

        if (config.containsKey(key)) {
          LOGGER.info(String.format("Overriding property from environment: %s=%s", key, value));
        } else {
          LOGGER.info(String.format("Setting property from environment: %s=%s", key, value));
        }

        envConfig.put(key, value);
      }
    }

    for (Map.Entry<String, String> entry: config.entrySet()) {
      String key = entry.getKey();

      if (!envConfig.containsKey(key) && envConfig.containsKey(key.toLowerCase())) {
        throw new IllegalArgumentException(String.format(
            "Can't override property from environment with mixed case: %s", key));
      }
    }

    return new MapConfig(Arrays.asList(config, envConfig));
  }

  protected static String renameKey(String envName) {
    if (envName.length() <= SAMZA_KEY_PREFIX.length()) {
      throw new IllegalArgumentException();
    }

    String configName = envName.substring(SAMZA_KEY_PREFIX.length());
    return configName.toLowerCase().replace('_', '.');
  }
}
