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

package org.apache.samza.system.inmemory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.EndOfStreamMessage;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class TestInMemorySystem {
  private static final String SYSTEM_NAME = "in-memory";
  private static final String STREAM_NAME = "test-stream";
  private static final String SOURCE = "test-in-memory-source";
  private static final int PARTITION_COUNT = 5;
  private static final int POLL_TIMEOUT_MS = 100;

  private static final int TEST_MEMBER_X = 1234;
  private static final int PAGE_ID_X = 3456;

  private static final int TEST_MEMBER_Y = 2345;
  private static final int PAGE_ID_Y = 2222;

  private static final SystemStream SYSTEM_STREAM = new SystemStream(SYSTEM_NAME, STREAM_NAME);

  private MetricsRegistry mockRegistry = mock(MetricsRegistry.class);
  private Config config = new MapConfig();

  private InMemorySystemFactory systemFactory;
  private SystemAdmin systemAdmin;

  public TestInMemorySystem() {
    Config config = new MapConfig();

    systemFactory = new InMemorySystemFactory();
    systemAdmin = systemFactory.getAdmin(SYSTEM_NAME, config);
    systemAdmin.createStream(new StreamSpec(STREAM_NAME, STREAM_NAME, SYSTEM_NAME, PARTITION_COUNT));
  }

  @Test
  public void testMessageFlow() {
    PageViewEvent event1 = new PageViewEvent(TEST_MEMBER_X, PAGE_ID_X, System.currentTimeMillis());
    PageViewEvent event2 = new PageViewEvent(TEST_MEMBER_Y, PAGE_ID_Y, System.currentTimeMillis());

    produceMessages(event1, event2);

    Set<SystemStreamPartition> sspsToPoll = IntStream.range(0, PARTITION_COUNT)
        .mapToObj(partition -> new SystemStreamPartition(SYSTEM_STREAM, new Partition(partition)))
        .collect(Collectors.toSet());

    List<PageViewEvent> results = consumeMessages(sspsToPoll);

    assertEquals(2, results.size());
    assertTrue(results.contains(event1));
    assertTrue(results.contains(event2));
  }

  @Test
  public void testConsumerRespectsOffset() {
    PageViewEvent event = new PageViewEvent(TEST_MEMBER_X, PAGE_ID_X, System.currentTimeMillis());
    PageViewEvent event1 = new PageViewEvent(TEST_MEMBER_Y, PAGE_ID_Y, System.currentTimeMillis());

    produceMessages(event);

    SystemConsumer consumer = systemFactory.getConsumer(SYSTEM_NAME, config, mockRegistry);

    Set<SystemStreamPartition> sspsToPoll = IntStream.range(0, PARTITION_COUNT)
        .mapToObj(partition -> new SystemStreamPartition(SYSTEM_STREAM, new Partition(partition)))
        .collect(Collectors.toSet());

    // register the consumer for ssps
    for (SystemStreamPartition ssp : sspsToPoll) {
      consumer.register(ssp, "0");
    }

    List<PageViewEvent> results = consumeMessages(consumer, sspsToPoll);
    assertEquals(1, results.size());
    assertTrue(results.contains(event));

    // nothing to poll
    results = consumeMessages(consumer, sspsToPoll);
    assertEquals(0, results.size());

    produceMessages(event1);

    // got new message. check if the offset has progressed
    results = consumeMessages(consumer, sspsToPoll);
    assertEquals(1, results.size());
    assertTrue(results.contains(event1));
  }

  @Test
  public void testEndOfStreamMessage() {
    EndOfStreamMessage eos = new EndOfStreamMessage("test-task");

    produceMessages(eos);

    Set<SystemStreamPartition> sspsToPoll = IntStream.range(0, PARTITION_COUNT)
        .mapToObj(partition -> new SystemStreamPartition(SYSTEM_STREAM, new Partition(partition)))
        .collect(Collectors.toSet());

    List<IncomingMessageEnvelope> results = consumeRawMessages(sspsToPoll);

    assertEquals(1, results.size());
    assertTrue(results.get(0).isEndOfStream());
  }


  private <T> List<T> consumeMessages(Set<SystemStreamPartition> sspsToPoll) {
    SystemConsumer systemConsumer = systemFactory.getConsumer(SYSTEM_NAME, config, mockRegistry);

    // register the consumer for ssps
    for (SystemStreamPartition ssp : sspsToPoll) {
      systemConsumer.register(ssp, "0");
    }

    return consumeMessages(systemConsumer, sspsToPoll);
  }

  private <T> List<T> consumeMessages(SystemConsumer consumer, Set<SystemStreamPartition> sspsToPoll) {
    return consumeRawMessages(consumer, sspsToPoll)
        .stream()
        .map(IncomingMessageEnvelope::getMessage)
        .map(message -> (T) message)
        .collect(Collectors.toList());
  }

  private List<IncomingMessageEnvelope> consumeRawMessages(Set<SystemStreamPartition> sspsToPoll) {
    SystemConsumer systemConsumer = systemFactory.getConsumer(SYSTEM_NAME, config, mockRegistry);

    // register the consumer for ssps
    for (SystemStreamPartition ssp : sspsToPoll) {
      systemConsumer.register(ssp, "0");
    }

    return consumeRawMessages(systemConsumer, sspsToPoll);
  }

  private List<IncomingMessageEnvelope> consumeRawMessages(SystemConsumer consumer, Set<SystemStreamPartition> sspsToPoll) {
    try {
      Map<SystemStreamPartition, List<IncomingMessageEnvelope>> results = consumer.poll(sspsToPoll, POLL_TIMEOUT_MS);

      return results.entrySet()
          .stream()
          .filter(entry -> entry.getValue().size() != 0)
          .map(Map.Entry::getValue)
          .flatMap(List::stream)
          .collect(Collectors.toList());
    } catch (Exception e) {
      fail("Unable to consume messages");
    }

    return new ArrayList<>();
  }

  private void produceMessages(Object... events) {
    SystemProducer systemProducer = systemFactory.getProducer(SYSTEM_NAME, config, mockRegistry);

    Stream.of(events)
        .forEach(event -> systemProducer.send(SOURCE, new OutgoingMessageEnvelope(SYSTEM_STREAM, event)));
  }

  private class PageViewEvent {
    int memberId;
    int pageId;
    long viewTime;

    public PageViewEvent(int memberId, int pageId, long viewTime) {
      this.memberId = memberId;
      this.pageId = pageId;
      this.viewTime = viewTime;
    }
  }
}
