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
package org.apache.samza.testUtils;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamOperatorTask;
import org.apache.samza.task.TaskCallback;
import org.apache.samza.task.TaskCoordinator;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class StreamTestUtils {

  /**
   * Adds the streams.stream-id.* configurations for the provided {@code streamId} to the provided {@code configs} Map.
   *
   * @param configs the configs Map to add the stream configurations to
   * @param streamId the id of the stream
   * @param systemName the system for the stream
   * @param physicalName the physical name for the stream
   */
  public static void addStreamConfigs(Map<String, String> configs,
      String streamId, String systemName, String physicalName) {
    configs.put(String.format(StreamConfig.SYSTEM_FOR_STREAM_ID, streamId), systemName);
    configs.put(String.format(StreamConfig.PHYSICAL_NAME_FOR_STREAM_ID, streamId), physicalName);
  }

  public static void processSync(StreamOperatorTask task, IncomingMessageEnvelope ime, MessageCollector collector,
      TaskCoordinator coordinator, TaskCallback callback) {
    final CountDownLatch latch = new CountDownLatch(1);
    doAnswer(invocation -> {
      latch.countDown();
      return null;
    }).when(callback).complete();
    task.processAsync(ime, collector, coordinator, callback);
    try {
      latch.await(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
    }
  }
}