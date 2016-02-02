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

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class EnvironmentConfigRewriterTest {
  private EnvironmentConfigRewriter rewriter;

  @Before
  public void setup() {
    rewriter = new EnvironmentConfigRewriter();
  }

  @Test
  public void testRewriteKeyValidKeys() {
    assertEquals("foo", EnvironmentConfigRewriter.renameKey("SAMZA_FOO"));
    assertEquals("foo.bar", EnvironmentConfigRewriter.renameKey("SAMZA_FOO_BAR"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRewriteKeyInvalidKeyPrefix() {
    EnvironmentConfigRewriter.renameKey("SAMZA");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRewriteKeyInvalidKeyNoSubkey() {
    EnvironmentConfigRewriter.renameKey("SAMZA");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRewriteFailsOnDowncaseMatch() throws Exception {
    Map<String, String> config = createMap("foo.Bar", "a");
    Map<String, String> env = createMap("SAMZA_FOO_BAR", "b");

    rewriter.rewrite(new MapConfig(config), env);
  }

  @Test
  public void testRewriteOverridesConfig() throws Exception {
    Map<String, String> config = createMap("foo.bar", "a");
    Map<String, String> env = createMap("SAMZA_FOO_BAR", "b");

    Config rewritten = rewriter.rewrite(new MapConfig(config), env);

    assertEquals("b", rewritten.get("foo.bar"));
  }

  private static Map<String, String> createMap(String key, String value) {
    Map<String, String> map = new HashMap<>();
    map.put(key, value);

    return map;
  }
}
