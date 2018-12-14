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
package org.apache.samza.startpoint;

import java.time.Instant;
import org.apache.samza.Partition;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Assert;
import org.junit.Test;


public class TestStartpoint {

  @Test
  public void testStartpointSpecific() {
    StartpointSpecific startpoint = new StartpointSpecific("123");
    Assert.assertEquals("123", startpoint.getSpecificOffset());
    Assert.assertTrue(startpoint.getCreatedTimestamp() <= Instant.now().toEpochMilli());

    MockStartpointConsumer mockStartpointConsumer = new MockStartpointConsumer();
    startpoint.apply(new SystemStreamPartition("sys", "stream", new Partition(1)), mockStartpointConsumer);
    Assert.assertEquals(StartpointSpecific.class, mockStartpointConsumer.visitedClass);
  }

  @Test
  public void testStartpointTimestamp() {
    StartpointTimestamp startpoint = new StartpointTimestamp(2222222L);
    Assert.assertEquals(2222222L, startpoint.getTimestampOffset().longValue());
    Assert.assertTrue(startpoint.getCreatedTimestamp() <= Instant.now().toEpochMilli());

    MockStartpointConsumer mockStartpointConsumer = new MockStartpointConsumer();
    startpoint.apply(new SystemStreamPartition("sys", "stream", new Partition(1)), mockStartpointConsumer);
    Assert.assertEquals(StartpointTimestamp.class, mockStartpointConsumer.visitedClass);
  }

  @Test
  public void testStartpointEarliest() {
    StartpointEarliest startpoint = new StartpointEarliest();
    Assert.assertTrue(startpoint.getCreatedTimestamp() <= Instant.now().toEpochMilli());

    MockStartpointConsumer mockStartpointConsumer = new MockStartpointConsumer();
    startpoint.apply(new SystemStreamPartition("sys", "stream", new Partition(1)), mockStartpointConsumer);
    Assert.assertEquals(StartpointEarliest.class, mockStartpointConsumer.visitedClass);
  }

  @Test
  public void testStartpointLatest() {
    StartpointLatest startpoint = new StartpointLatest();
    Assert.assertTrue(startpoint.getCreatedTimestamp() <= Instant.now().toEpochMilli());

    MockStartpointConsumer mockStartpointConsumer = new MockStartpointConsumer();
    startpoint.apply(new SystemStreamPartition("sys", "stream", new Partition(1)), mockStartpointConsumer);
    Assert.assertEquals(StartpointLatest.class, mockStartpointConsumer.visitedClass);
  }

  @Test
  public void testStartpointBootstrap() {
    StartpointBootstrap startpoint = new StartpointBootstrap("test12345");
    Assert.assertEquals("test12345", startpoint.getBootstrapInfo());
    Assert.assertTrue(startpoint.getCreatedTimestamp() <= Instant.now().toEpochMilli());

    MockStartpointConsumer mockStartpointConsumer = new MockStartpointConsumer();
    startpoint.apply(new SystemStreamPartition("sys", "stream", new Partition(1)), mockStartpointConsumer);
    Assert.assertEquals(StartpointBootstrap.class, mockStartpointConsumer.visitedClass);
  }

  static class MockStartpointConsumer implements StartpointConsumerVisitor {
    Class<? extends Startpoint> visitedClass;

    @Override
    public void register(SystemStreamPartition systemStreamPartition, StartpointSpecific startpointSpecific) {
      visitedClass = startpointSpecific.getClass();
    }

    @Override
    public void register(SystemStreamPartition systemStreamPartition, StartpointTimestamp startpointTimestamp) {
      visitedClass = startpointTimestamp.getClass();
    }

    @Override
    public void register(SystemStreamPartition systemStreamPartition, StartpointEarliest startpointEarliest) {
      visitedClass = startpointEarliest.getClass();
    }

    @Override
    public void register(SystemStreamPartition systemStreamPartition, StartpointLatest startpointLatest) {
      visitedClass = startpointLatest.getClass();
    }

    @Override
    public void register(SystemStreamPartition systemStreamPartition, StartpointBootstrap startpointBootstrap) {
      visitedClass = startpointBootstrap.getClass();
    }
  }
}
