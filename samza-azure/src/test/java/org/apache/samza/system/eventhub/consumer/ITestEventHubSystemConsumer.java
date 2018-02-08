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

package org.apache.samza.system.eventhub.consumer;

import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.eventhub.EventHubSystemFactory;
import org.apache.samza.system.eventhub.MockEventHubConfigFactory;
import org.apache.samza.system.eventhub.TestMetricsRegistry;
import org.apache.samza.system.eventhub.producer.EventHubSystemProducer;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;

import static org.apache.samza.system.eventhub.MockEventHubConfigFactory.STREAM_NAME1;
import static org.apache.samza.system.eventhub.MockEventHubConfigFactory.SYSTEM_NAME;

@Ignore("Requires Azure account credentials")
public class ITestEventHubSystemConsumer {

  private Config createEventHubConfig() {
    return MockEventHubConfigFactory.getEventHubConfig(EventHubSystemProducer.PartitioningMethod.EVENT_HUB_HASHING);
  }

  @Test
  public void testSinglePartitionConsumptionHappyPath() throws Exception {
    int partitionId = 0;

    TestMetricsRegistry testMetrics = new TestMetricsRegistry();
    SystemStreamPartition ssp = new SystemStreamPartition(SYSTEM_NAME, STREAM_NAME1, new Partition(partitionId));

    Config eventHubConfig = createEventHubConfig();

    EventHubSystemFactory factory = new EventHubSystemFactory();
    SystemConsumer consumer = factory.getConsumer(SYSTEM_NAME, eventHubConfig, testMetrics);
    consumer.register(ssp, EventHubSystemConsumer.START_OF_STREAM);
    consumer.start();

    int numEvents = 0;
    int numRetries = 20;
    while (numRetries-- > 0) {
      List<IncomingMessageEnvelope> result = consumer.poll(Collections.singleton(ssp), 2000).get(ssp);
      numEvents = result == null ? 0 : result.size();
      if (numEvents > 0) {
        EventHubIncomingMessageEnvelope eventData = (EventHubIncomingMessageEnvelope) result.get(0);
        System.out.println("System properties: " + eventData.getEventData().getSystemProperties());
        System.out.println("Message: " + new String((byte[]) eventData.getMessage()));
        break;
      }
      System.out.println("Retries left: " + numRetries);
    }
    Assert.assertTrue(numEvents > 0);
  }

}
