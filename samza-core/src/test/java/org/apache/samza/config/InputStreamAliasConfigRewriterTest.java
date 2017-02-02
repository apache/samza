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

import junit.framework.Assert;
import org.apache.samza.SamzaException;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class InputStreamAliasConfigRewriterTest {
  private Map<String, String> beforeRewrite = null;
  private Map<String, String> afterRewrite = null;
  private InputStreamAliasConfigRewriter rewriter = new InputStreamAliasConfigRewriter();
  private ExpectedException exception = ExpectedException.none();

  @Before
  public void setup() {
    Map<String, String> configMap = new HashMap<>();

    configMap.put("task.inputs", "kafka.my-stream-1 as broad-alias");
    configMap.put("task.broadcast.inputs", "kafka.broad-alias#0");
    configMap.put("systems.kafka.streams.broad-alias.samza.key.serde", "string");
    configMap.put("systems.kafka.streams.broad-alias.samza.msg.serde", "json");
    configMap.put("systems.kafka.streams.broad-alias.samza.bootstrap", "true");
    configMap.put("systems.kafka.streams.broad-alias.samza.offset.default", "oldest");
    beforeRewrite = new HashMap<>(configMap);

    configMap.clear();
    configMap.put("task.inputs", "kafka.my-stream-1");
    configMap.put("task.broadcast.inputs", "kafka.my-stream-1#0");
    configMap.put("systems.kafka.streams.my-stream-1.samza.key.serde", "string");
    configMap.put("systems.kafka.streams.my-stream-1.samza.msg.serde", "json");
    configMap.put("systems.kafka.streams.my-stream-1.samza.bootstrap", "true");
    configMap.put("systems.kafka.streams.my-stream-1.samza.offset.default", "oldest");
    afterRewrite = new HashMap(configMap);
  }

  @Test
  public void testOneAlias() {
    assertEquals(afterRewrite, rewriter.rewrite(new MapConfig(beforeRewrite)));
  }

  @Test
  public void testSeveralAliases() {
    beforeRewrite.put("task.inputs", "kafka.my-stream-1 as broad-alias, kafka.my-stream-2 as alias2");
    beforeRewrite.put("task.broadcast.inputs", "kafka.broad-alias#0, kafka.alias2#[0-5]");
    beforeRewrite.put("systems.kafka.streams.alias2.samza.bootstrap", "true");

    afterRewrite.put("task.inputs", "kafka.my-stream-1, kafka.my-stream-2");
    afterRewrite.put("task.broadcast.inputs", "kafka.my-stream-1#0, kafka.my-stream-2#[0-5]");
    afterRewrite.put("systems.kafka.streams.my-stream-2.samza.bootstrap", "true");

    assertEquals(afterRewrite, rewriter.rewrite(new MapConfig(beforeRewrite)));
  }

  @Test
  public void testFailOnSystemAndStreamNotSeparated() throws Exception {
    beforeRewrite.put("task.inputs", "kafkamy-stream-1 as broadcast");

    boolean testResult = false;
    try {
      rewriter.rewrite(new MapConfig(beforeRewrite));
    } catch (SamzaException se) {
      Throwable cause = se.getCause();
      if (cause != null && (cause instanceof ParseException)) {
        assertEquals("Could not parse alias for input stream. Original stream should be in format 'system-name.stream-name'", cause.getMessage());
        testResult = true;
      }
    }
    if (!testResult)
      Assert.fail();
  }

  @Test(expected = SamzaException.class)
  public void testValueWordCheckFail() throws Exception {
    Map<String, String> configMap = new HashMap<>();

    configMap.put("task.inputs", "kafka.my-stream-2 as stri");
    configMap.put("systems.kafka.streams.broad-alias.samza.key.serde", "string");

    exception.expect(SamzaException.class);
    exception.expectMessage("Alias for input stream ('stri') cannot be a part of config value word ('string')");

    rewriter.rewrite(new MapConfig(configMap));
  }

  @Test(expected = SamzaException.class)
  public void testKeyWordCheckFail() throws Exception {
    Map<String, String> configMap = new HashMap<>();

    configMap.put("task.inputs", "kafka.my-stream-2 as str");
    configMap.put("systems.kafka.streams.broad-alias.samza.key.serde", "json");

    exception.expect(SamzaException.class);
    exception.expectMessage("Alias for input stream ('str') cannot be a part of config key word ('streams')");

    rewriter.rewrite(new MapConfig(configMap));
  }

  @Test
  public void testNoAliasesUsed() {
    Map<String, String> configMap = new HashMap<>();

    configMap.put("task.inputs", "kafka.my-stream");
    configMap.put("systems.kafka.streams.my-stream.samza.key.serde", "string");
    MapConfig config = new MapConfig(configMap);
    Config rewritenConfig = rewriter.rewrite(config);
    assertSame(config, rewritenConfig);
  }
}