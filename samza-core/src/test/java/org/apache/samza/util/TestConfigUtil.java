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
import org.apache.samza.config.ConfigRewriter;
import org.apache.samza.config.MapConfig;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


public class TestConfigUtil {
  Map<String, String> configMap = new HashMap<>();

  @Before
  public void setup() {
    configMap.put("job.config.rewriter.testRewriter.class", TestConfigRewriter.class.getName());
    configMap.put("job.config.rewriter.testNoneRewriter.class", "");

  }

  @Test
  public void testRewriterWithConfigRewriter() {
    configMap.put("job.config.rewriters", "testRewriter");
    configMap.put("job.config.rewriter.testRewriter.value", "rewrittenTest");

    Config config = ConfigUtil.rewriteConfig(new MapConfig(configMap));
    assertEquals("rewrittenTest", config.get("value"));
  }

  @Test
  public void testGetRewriterWithoutConfigRewriter() {
    Config config = ConfigUtil.rewriteConfig(new MapConfig(configMap));
    assertEquals(config, new MapConfig(configMap));
  }

  @Test (expected = RuntimeException.class)
  public void testGetRewriterWithExceptoion() {
    configMap.put("job.config.rewriters", "testNoneRewriter");
    ConfigUtil.rewriteConfig(new MapConfig(configMap));
  }

  public static class TestConfigRewriter implements ConfigRewriter {
    @Override
    public Config rewrite(String name, Config config) {
      Map<String, String> configMap = new HashMap<>(config);
      configMap.putAll(config.subset(String.format("job.config.rewriter.%s.", name)));
      return new MapConfig(configMap);
    }
  }
}
