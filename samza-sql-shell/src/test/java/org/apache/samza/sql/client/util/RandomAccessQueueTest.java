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

package org.apache.samza.sql.client.util;

import java.util.List;
import org.junit.Assert;
import org.junit.Test;


public class RandomAccessQueueTest {
  private RandomAccessQueue mQueue;
  public RandomAccessQueueTest() {
    mQueue = new RandomAccessQueue<>(Integer.class, 5);
  }

  @Test
  public void testAddAndGetElement() {
    mQueue.clear();
    for (int i = 0; i < 4; i++) {
      mQueue.add(i);
    }
    Assert.assertEquals(0, mQueue.getHead());
    Assert.assertEquals(4, mQueue.getSize());
    Assert.assertEquals(0, mQueue.get(0));
    Assert.assertEquals(3, mQueue.get(3));

    for (int i = 0; i < 3; i++) {
      mQueue.add(4 + i);
    }
    int head = mQueue.getHead();
    Assert.assertEquals(2, head);
    Assert.assertEquals(5, mQueue.getSize());
    Assert.assertEquals(2, mQueue.get(0));
    Assert.assertEquals(3, mQueue.get(1));
    Assert.assertEquals(4, mQueue.get(2));
    Assert.assertEquals(5, mQueue.get(3));
    Assert.assertEquals(6, mQueue.get(4));
  }

  @Test
  public void testGetRange() {
    mQueue.clear();
    for (int i = 0; i < 4; i++) {
      mQueue.add(i); // 0, 1, 2, 3
    }
    List<Integer> rets = mQueue.get(-1, 9);
    Assert.assertEquals(4, rets.size());
    Assert.assertEquals(0, mQueue.get(0));
    Assert.assertEquals(3, mQueue.get(3));

    for (int i = 0; i <= 2; i++) {
      mQueue.add(4 + i);
    }
    rets = mQueue.get(0, 4);
    Assert.assertEquals(2, rets.get(0).intValue());
    Assert.assertEquals(3, rets.get(1).intValue());
    Assert.assertEquals(4, rets.get(2).intValue());
    Assert.assertEquals(5, rets.get(3).intValue());
    Assert.assertEquals(6, rets.get(4).intValue());
  }

  @Test
  public void testConsume() {
    mQueue.clear();
    for (int i = 0; i < 4; i++) {
      mQueue.add(i); // 0, 1, 2, 3
    }
    List<Integer> rets = mQueue.consume(1, 2);
    Assert.assertEquals(1, mQueue.getSize());
    Assert.assertEquals(3, mQueue.getHead());
  }
}