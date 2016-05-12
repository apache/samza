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

package org.apache.samza.coordinator.stream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.coordinator.stream.messages.CoordinatorStreamMessage;
import org.apache.samza.coordinator.stream.messages.Delete;
import org.apache.samza.coordinator.stream.messages.SetConfig;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCoordinatorStreamSystemConsumer {
  @Test
  public void testCoordinatorStreamSystemConsumer() {
    Map<String, String> expectedConfig = new LinkedHashMap<String, String>();
    expectedConfig.put("job.id", "1234");
    SystemStream systemStream = new SystemStream("system", "stream");
    MockSystemConsumer systemConsumer = new MockSystemConsumer(new SystemStreamPartition(systemStream, new Partition(0)));
    CoordinatorStreamSystemConsumer consumer = new CoordinatorStreamSystemConsumer(systemStream, systemConsumer, new SinglePartitionWithoutOffsetsSystemAdmin());
    assertEquals(0, systemConsumer.getRegisterCount());
    consumer.register();
    assertEquals(1, systemConsumer.getRegisterCount());
    assertFalse(systemConsumer.isStarted());
    consumer.start();
    assertTrue(systemConsumer.isStarted());
    try {
      consumer.getConfig();
      fail("Should have failed when retrieving config before bootstrapping.");
    } catch (SamzaException e) {
      // Expected.
    }
    consumer.bootstrap();
    assertEquals(expectedConfig, consumer.getConfig());
    assertFalse(systemConsumer.isStopped());
    consumer.stop();
    assertTrue(systemConsumer.isStopped());
  }

  @Test
  public void testCoordinatorStreamSystemConsumerRegisterOnceOnly() throws Exception {
    Map<String, String> expectedConfig = new LinkedHashMap<String, String>();
    expectedConfig.put("job.id", "1234");
    SystemStream systemStream = new SystemStream("system", "stream");
    MockSystemConsumer systemConsumer = new MockSystemConsumer(new SystemStreamPartition(systemStream, new Partition(0)));
    CoordinatorStreamSystemConsumer consumer = new CoordinatorStreamSystemConsumer(systemStream, systemConsumer, new SinglePartitionWithoutOffsetsSystemAdmin());
    assertEquals(0, systemConsumer.getRegisterCount());
    consumer.register();
    assertEquals(1, systemConsumer.getRegisterCount());
    assertFalse(systemConsumer.isStarted());
    consumer.start();
    assertTrue(systemConsumer.isStarted());
    consumer.register();
    assertEquals(1, systemConsumer.getRegisterCount());
  }

  /**
   * Verify that if a particular key-value is written, then another, then the original again,
   * that the original occurs last in the set.
   */
  @Test
  public void testOrderKeyRewrite() throws InterruptedException {
    final SystemStream systemStream = new SystemStream("system", "stream");
    final SystemStreamPartition ssp = new SystemStreamPartition(systemStream, new Partition(0));
    final SystemConsumer systemConsumer = mock(SystemConsumer.class);

    final List<IncomingMessageEnvelope> list = new ArrayList<>();
    SetConfig setConfig1 = new SetConfig("source", "key1", "value1");
    SetConfig setConfig2 = new SetConfig("source", "key1", "value2");
    SetConfig setConfig3 = new SetConfig("source", "key1", "value1");
    list.add(createIncomingMessageEnvelope(setConfig1, ssp));
    list.add(createIncomingMessageEnvelope(setConfig2, ssp));
    list.add(createIncomingMessageEnvelope(setConfig3, ssp));
    Map<SystemStreamPartition, List<IncomingMessageEnvelope>> messages = new HashMap<SystemStreamPartition, List<IncomingMessageEnvelope>>() {
      {
        put(ssp, list);
      }
    };
    when(systemConsumer.poll(anySet(), anyLong())).thenReturn(messages, Collections.<SystemStreamPartition, List<IncomingMessageEnvelope>>emptyMap());

    CoordinatorStreamSystemConsumer consumer = new CoordinatorStreamSystemConsumer(systemStream, systemConsumer, new SinglePartitionWithoutOffsetsSystemAdmin());

    consumer.bootstrap();

    Set<CoordinatorStreamMessage> bootstrappedMessages = consumer.getBoostrappedStream();

    assertEquals(2, bootstrappedMessages.size()); // First message should have been removed as a duplicate
    CoordinatorStreamMessage[] coordinatorStreamMessages = bootstrappedMessages.toArray(new CoordinatorStreamMessage[2]);
    assertEquals(setConfig2, coordinatorStreamMessages[0]);
    assertEquals(setConfig3, coordinatorStreamMessages[1]); //Config 3 MUST be the last message, not config 2
  }

  private static class MockSystemConsumer implements SystemConsumer {
    private boolean started = false;
    private boolean stopped = false;
    private int registerCount = 0;
    private final SystemStreamPartition expectedSystemStreamPartition;
    private int pollCount = 0;

    public MockSystemConsumer(SystemStreamPartition expectedSystemStreamPartition) {
      this.expectedSystemStreamPartition = expectedSystemStreamPartition;
    }

    public void start() {
      started = true;
    }

    public void stop() {
      stopped = true;
    }

    public void register(SystemStreamPartition systemStreamPartition, String offset) {
      registerCount++;
      assertEquals(expectedSystemStreamPartition, systemStreamPartition);
    }

    public int getRegisterCount() {
      return registerCount;
    }

    public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(Set<SystemStreamPartition> systemStreamPartitions, long timeout) throws InterruptedException {
      Map<SystemStreamPartition, List<IncomingMessageEnvelope>> map = new LinkedHashMap<SystemStreamPartition, List<IncomingMessageEnvelope>>();
      assertEquals(1, systemStreamPartitions.size());
      SystemStreamPartition systemStreamPartition = systemStreamPartitions.iterator().next();
      assertEquals(expectedSystemStreamPartition, systemStreamPartition);

      if (pollCount++ == 0) {
        List<IncomingMessageEnvelope> list = new ArrayList<IncomingMessageEnvelope>();
        SetConfig setConfig1 = new SetConfig("test", "job.name", "my-job-name");
        SetConfig setConfig2 = new SetConfig("test", "job.id", "1234");
        Delete delete = new Delete("test", "job.name", SetConfig.TYPE);
        list.add(new IncomingMessageEnvelope(systemStreamPartition, null, serialize(setConfig1.getKeyArray()), serialize(setConfig1.getMessageMap())));
        list.add(new IncomingMessageEnvelope(systemStreamPartition, null, serialize(setConfig2.getKeyArray()), serialize(setConfig2.getMessageMap())));
        list.add(new IncomingMessageEnvelope(systemStreamPartition, null, serialize(delete.getKeyArray()), delete.getMessageMap()));
        map.put(systemStreamPartition, list);
      }

      return map;
    }

    private byte[] serialize(Object obj) {
      try {
        return SamzaObjectMapper.getObjectMapper().writeValueAsString(obj).getBytes("UTF-8");
      } catch (Exception e) {
        throw new SamzaException(e);
      }
    }

    public boolean isStarted() {
      return started;
    }

    public boolean isStopped() {
      return stopped;
    }
  }

  private IncomingMessageEnvelope createIncomingMessageEnvelope(CoordinatorStreamMessage message, SystemStreamPartition ssp) {
    try {
      byte[] key = SamzaObjectMapper.getObjectMapper().writeValueAsString(message.getKeyArray()).getBytes("UTF-8");
      byte[] value = SamzaObjectMapper.getObjectMapper().writeValueAsString(message.getMessageMap()).getBytes("UTF-8");
      return new IncomingMessageEnvelope(ssp, null, key, value);
    } catch (Exception e) {
      return null;
    }
  }
}
