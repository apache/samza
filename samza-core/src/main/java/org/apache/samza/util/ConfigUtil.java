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
import java.util.Optional;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.ConfigLoader;
import org.apache.samza.config.ConfigLoaderFactory;
import org.apache.samza.config.ConfigRewriter;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConfigUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigUtil.class);

  /**
   * Re-writes configuration using a ConfigRewriter, if one is defined. If
   * there is no ConfigRewriter defined for the job, then this method is a
   * no-op.
   *
   * @param config The config to re-write
   * @return rewrited configs
   */
  public static Config rewriteConfig(Config config) {
    Optional<String> configRewriterNamesOptional = new JobConfig(config).getConfigRewriters();
    if (configRewriterNamesOptional.isPresent()) {
      String[] configRewriterNames = configRewriterNamesOptional.get().split(",");
      Config rewrittenConfig = config;
      for (String configRewriterName : configRewriterNames) {
        rewrittenConfig = applyRewriter(rewrittenConfig, configRewriterName);
      }
      return rewrittenConfig;
    } else {
      return config;
    }
  }

  /**
   * Re-writes configuration using a ConfigRewriter, defined with the given rewriterName in config.
   * @param config the config to re-write
   * @param rewriterName the name of the rewriter to apply
   * @return the rewritten config
   */
  public static Config applyRewriter(Config config, String rewriterName) {
    String rewriterClassName = new JobConfig(config).getConfigRewriterClass(rewriterName)
        .orElseThrow(() -> new SamzaException(
            String.format("Unable to find class config for config rewriter %s.", rewriterName)));
    ConfigRewriter rewriter = ReflectionUtil.getObj(rewriterClassName, ConfigRewriter.class);
    LOG.info("Re-writing config with {}", rewriter);
    return rewriter.rewrite(rewriterName, config);
  }

  /**
   * Load full job config with {@link ConfigLoaderFactory} when present.
   *
   * @param original config
   * @return full job config
   */
  public static Config loadConfig(Config original) {
    JobConfig jobConfig = new JobConfig(original);

    if (!jobConfig.getConfigLoaderFactory().isPresent()) {
      throw new ConfigException("Missing key " + JobConfig.CONFIG_LOADER_FACTORY + ".");
    }

    ConfigLoaderFactory factory = ReflectionUtil.getObj(jobConfig.getConfigLoaderFactory().get(), ConfigLoaderFactory.class);
    ConfigLoader loader = factory.getLoader(original.subset(ConfigLoaderFactory.CONFIG_LOADER_PROPERTIES_PREFIX));
    // overrides config loaded with original config, which may contain overridden values.
    return rewriteConfig(override(loader.getConfig(), original));
  }

  /**
   * Overrides original config with overridden values.
   *
   * @param original config to be overridden.
   * @param overrides overridden values.
   * @return the overridden config.
   */
  @SafeVarargs
  private static Config override(Config original, Map<String, String>... overrides) {
    Map<String, String> map = new HashMap<>(original);

    for (Map<String, String> override : overrides) {
      map.putAll(override);
    }

    return new MapConfig(map);
  }
}
