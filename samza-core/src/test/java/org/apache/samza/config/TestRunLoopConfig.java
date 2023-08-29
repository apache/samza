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

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.junit.Assert.*;


public class TestRunLoopConfig {

  @Test
  public void testWatermarkCallbackTimeoutDefaultsToTaskCallbackTimeout() {
    long taskCallbackTimeout = 10L;
    Config config = new MapConfig(ImmutableMap.of(TaskConfig.CALLBACK_TIMEOUT_MS, Long.toString(taskCallbackTimeout)));
    RunLoopConfig runLoopConfig = new RunLoopConfig(config);
    assertEquals("Watermark callback timeout should default to task callback timeout",
        taskCallbackTimeout, runLoopConfig.getWatermarkCallbackTimeoutMs());
  }

  @Test
  public void testWatermarkCallbackTimeout() {
    long taskCallbackTimeout = 10L;
    long watermarkCallbackTimeout = 20L;
    Config config = new MapConfig(ImmutableMap.of(
        TaskConfig.CALLBACK_TIMEOUT_MS, Long.toString(taskCallbackTimeout),
        TaskConfig.WATERMARK_CALLBACK_TIMEOUT_MS, Long.toString(watermarkCallbackTimeout)));

    RunLoopConfig runLoopConfig = new RunLoopConfig(config);
    assertEquals("Mismatch in watermark callback timeout",
        watermarkCallbackTimeout, runLoopConfig.getWatermarkCallbackTimeoutMs());
  }
}
