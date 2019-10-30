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
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigRewriter;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;


public class ConfigUtil {
  /**
   * Re-writes configuration using a ConfigRewriter, if one is defined. If
   * there is no ConfigRewriter defined for the job, then this method is a
   * no-op.
   *
   * @param config The config to re-write
   * @return rewrited configs
   */
  static public Config rewriteConfig(Config config) {
    try {
      final String rewriters = config.get(JobConfig.CONFIG_REWRITERS, "");
      if (!rewriters.isEmpty()) {
        Map<String, String> resultConfig = new HashMap<>(config);
        for (String rewriter : rewriters.split(",")) {
          String rewriterClassCfg = String.format(JobConfig.CONFIG_REWRITER_CLASS, rewriter);
          String rewriterClass = config.get(rewriterClassCfg, "");
          if (rewriterClass.isEmpty()) {
            throw new SamzaException(
                "Unable to find class config for config rewriter: " + rewriterClassCfg);
          }
          ConfigRewriter configRewriter = (ConfigRewriter) Class.forName(rewriterClass).newInstance();
          Config rewritedConfig = configRewriter.rewrite(rewriter, config);
          resultConfig.putAll(rewritedConfig);
        }
        return new MapConfig(resultConfig);
      } else {
        return config;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
