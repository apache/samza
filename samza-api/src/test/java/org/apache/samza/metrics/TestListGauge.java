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
package org.apache.samza.metrics;

import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import org.junit.Assert;
import org.junit.Test;


/**
 * Class to encapsulate test-cases for {@link org.apache.samza.metrics.ListGauge}
 */
public class TestListGauge {

  private final static Duration THREAD_TEST_TIMEOUT = Duration.ofSeconds(10);

  @Test
  public void basicTest() {
    ListGauge<String> listGauge = new ListGauge<String>("sampleListGauge");
    listGauge.add("sampleValue");
    Assert.assertEquals("Names should be the same", listGauge.getName(), "sampleListGauge");
    Assert.assertEquals("List sizes should match", listGauge.getValue().size(), 1);
    Assert.assertEquals("ListGauge should contain sampleGauge", listGauge.getValue().contains("sampleValue"), true);
  }

  @Test
  public void testSizeEnforcement() {
    ListGauge listGauge = new ListGauge<String>("listGauge");
    listGauge.setEvictionPolicy(new RetainLastNPolicy(listGauge, 10));
    for (int i = 15; i > 0; i--) {
      listGauge.add("v" + i);
    }
    Assert.assertEquals("List sizes should be as configured at creation time", listGauge.getValue().size(), 10);

    int valueIndex = 10;
    Collection<String> currentList = listGauge.getValue();
    Iterator iterator = currentList.iterator();
    while (iterator.hasNext()) {
      String gaugeValue = (String) iterator.next();
      Assert.assertTrue(gaugeValue.equals("v" + valueIndex));
      valueIndex--;
    }
  }

  @Test
  public void testThreadSafety() throws InterruptedException {
    ListGauge<Integer> listGauge = new ListGauge<Integer>("listGauge");
    listGauge.setEvictionPolicy(new RetainLastNPolicy(listGauge, 20));

    Thread thread1 = new Thread(new Runnable() {
      @Override
      public void run() {
        for (int i = 1; i <= 100; i++) {
          listGauge.add(i);
        }
      }
    });

    Thread thread2 = new Thread(new Runnable() {
      @Override
      public void run() {
        for (int i = 1; i <= 100; i++) {
          listGauge.add(i);
        }
      }
    });

    thread1.start();
    thread2.start();

    thread1.join(THREAD_TEST_TIMEOUT.toMillis());
    thread2.join(THREAD_TEST_TIMEOUT.toMillis());

    Assert.assertTrue("ListGauge should have the last 20 values", listGauge.getValue().size() == 20);
    for (Integer gaugeValue : listGauge.getValue()) {
      Assert.assertTrue("Values should have the last 20 range", gaugeValue <= 100 && gaugeValue > 80);
    }
  }
}
