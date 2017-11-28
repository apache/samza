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

package org.apache.samza.system.kinesis.consumer;

import org.junit.Assert;
import org.junit.Test;


public class TestKinesisSystemConsumerOffset {
  @Test
  public void testEquality() {
    KinesisSystemConsumerOffset inCkpt = new KinesisSystemConsumerOffset("shard-00000", "123456");
    KinesisSystemConsumerOffset outCkpt = KinesisSystemConsumerOffset.parse(inCkpt.toString());
    Assert.assertEquals(inCkpt, outCkpt);
  }

  @Test
  public void testInEquality() {
    KinesisSystemConsumerOffset inCkpt = new KinesisSystemConsumerOffset("shard-00000", "123456");

    // With different shardId
    KinesisSystemConsumerOffset inCkpt1 = new KinesisSystemConsumerOffset("shard-00001", "123456");
    KinesisSystemConsumerOffset outCkpt = KinesisSystemConsumerOffset.parse(inCkpt1.toString());
    Assert.assertTrue(!inCkpt.equals(outCkpt));

    // With different seqNumber
    inCkpt1 = new KinesisSystemConsumerOffset("shard-00000", "123457");
    outCkpt = KinesisSystemConsumerOffset.parse(inCkpt1.toString());
    Assert.assertTrue(!inCkpt.equals(outCkpt));
  }
}
