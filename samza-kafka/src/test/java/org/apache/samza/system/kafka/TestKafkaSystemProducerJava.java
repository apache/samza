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

import java.util.HashMap;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.util.ExponentialSleepStrategy;
import org.junit.Test;
import scala.runtime.AbstractFunction0;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;


/**
 * Tests the instantiation of a KafkaSystemProducer from Java clients
 */
public class TestKafkaSystemProducerJava {
  @Test
  public void testInstantiateProducer() {
    KafkaSystemProducer ksp = new KafkaSystemProducer("SysName", new ExponentialSleepStrategy(2.0, 200, 10000),
        new AbstractFunction0<Producer<byte[], byte[]>>() {
          @Override
          public Producer<byte[], byte[]> apply() {
            return new KafkaProducer<>(new HashMap<String, Object>());
          }
        }, new KafkaSystemProducerMetrics("SysName", new MetricsRegistryMap()), new AbstractFunction0<Object>() {
      @Override
      public Object apply() {
        return System.currentTimeMillis();
      }
    });

    // Default value should have been used.
    assertEquals(30, ksp.maxRetries());
    long now = System.currentTimeMillis();
    assertTrue((Long)ksp.clock().apply() >= now);
  }
}