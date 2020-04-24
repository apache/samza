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

package org.apache.samza.system.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;

import static org.mockito.Mockito.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;



public class TestKafkaConsumerProxy {

  @Test
  public void testSynchronizedAccessOfKafkaConsumer() throws Exception {
    KafkaConsumer mockKafkaConsumer = mock(KafkaConsumer.class);
    Thread t1 = getThreadToCreateStartStopProxy(mockKafkaConsumer);
    Thread t2 = getThreadToCreateStartStopProxy(mockKafkaConsumer);
    t1.start();
    t2.start();
    t1.join(600);
    t2.join(600);
    verify(mockKafkaConsumer, times(2)).endOffsets(anyCollection());
  }

  private Thread getThreadToCreateStartStopProxy(KafkaConsumer mockKafkaConsumer) {
    Thread t = new Thread() {
      @Override
      public void run() {
        KafkaConsumerProxy kafkaConsumerProxy1 = spy(new KafkaConsumerProxy(mock(KafkaSystemConsumer.class), mockKafkaConsumer,
            "systemName", "clientId", mock(KafkaSystemConsumer.KafkaConsumerMessageSink.class),
            mock(KafkaSystemConsumerMetrics.class), "metricName"));
        kafkaConsumerProxy1.start();
        kafkaConsumerProxy1.stop(60);
        verify(kafkaConsumerProxy1).initializeLags();
      }
    };
    return t;
  }
}
