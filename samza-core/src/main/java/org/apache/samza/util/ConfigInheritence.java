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

package org.apache.samza.util;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigInheritence {
  private static final Logger log = LoggerFactory.getLogger(ConfigInheritence.class);
  private static final boolean INHERIT_ROOT_CONFIGS = true;

  public static Config extractScopedConfig(Config fullConfig, Config generatedConfig, String configPrefix) {
    Config scopedConfig = fullConfig.subset(configPrefix);
    log.debug("Prefix '{}' has extracted config {}", configPrefix, scopedConfig);
    log.debug("Prefix '{}' has generated config {}", configPrefix, generatedConfig);

    Config[] configPrecedence;
    if (INHERIT_ROOT_CONFIGS) {
      configPrecedence = new Config[] {fullConfig, generatedConfig, scopedConfig};
    } else {
      configPrecedence = new Config[] {generatedConfig, scopedConfig};
    }

    // Strip empty configs so they don't override the configs before them.
    Map<String, String> mergedConfig = new HashMap<>();
    for (Map<String, String> config : configPrecedence) {
      for (Map.Entry<String, String> property : config.entrySet()) {
        String value = property.getValue();
        if (!(value == null || value.isEmpty())) {
          mergedConfig.put(property.getKey(), property.getValue());
        }
      }
    }
    scopedConfig = new MapConfig(mergedConfig);
    log.debug("Prefix '{}' has merged config {}", configPrefix, scopedConfig);

    return scopedConfig;
  }
}
