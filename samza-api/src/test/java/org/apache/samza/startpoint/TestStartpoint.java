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
    Assert.assertTrue(startpoint.getCreationTimestamp() <= Instant.now().toEpochMilli());

    MockStartpointVisitor mockStartpointVisitorConsumer = new MockStartpointVisitor();
    startpoint.apply(new SystemStreamPartition("sys", "stream", new Partition(1)), mockStartpointVisitorConsumer);
    Assert.assertEquals(StartpointSpecific.class, mockStartpointVisitorConsumer.visitedClass);
  }

  @Test
  public void testStartpointTimestamp() {
    StartpointTimestamp startpoint = new StartpointTimestamp(2222222L);
    Assert.assertEquals(2222222L, startpoint.getTimestampOffset().longValue());
    Assert.assertTrue(startpoint.getCreationTimestamp() <= Instant.now().toEpochMilli());

    MockStartpointVisitor mockStartpointVisitorConsumer = new MockStartpointVisitor();
    startpoint.apply(new SystemStreamPartition("sys", "stream", new Partition(1)), mockStartpointVisitorConsumer);
    Assert.assertEquals(StartpointTimestamp.class, mockStartpointVisitorConsumer.visitedClass);
  }

  @Test
  public void testStartpointEarliest() {
    StartpointOldest startpoint = new StartpointOldest();
    Assert.assertTrue(startpoint.getCreationTimestamp() <= Instant.now().toEpochMilli());

    MockStartpointVisitor mockStartpointVisitorConsumer = new MockStartpointVisitor();
    startpoint.apply(new SystemStreamPartition("sys", "stream", new Partition(1)), mockStartpointVisitorConsumer);
    Assert.assertEquals(StartpointOldest.class, mockStartpointVisitorConsumer.visitedClass);
  }

  @Test
  public void testStartpointLatest() {
    StartpointUpcoming startpoint = new StartpointUpcoming();
    Assert.assertTrue(startpoint.getCreationTimestamp() <= Instant.now().toEpochMilli());

    MockStartpointVisitor mockStartpointVisitorConsumer = new MockStartpointVisitor();
    startpoint.apply(new SystemStreamPartition("sys", "stream", new Partition(1)), mockStartpointVisitorConsumer);
    Assert.assertEquals(StartpointUpcoming.class, mockStartpointVisitorConsumer.visitedClass);
  }

  @Test
  public void testStartpointCustom() {
    MockStartpointCustom startpoint = new MockStartpointCustom("test12345", 12345);
    Assert.assertEquals("test12345", startpoint.getTestInfo1());
    Assert.assertEquals(12345, startpoint.getTestInfo2());
    Assert.assertTrue(startpoint.getCreationTimestamp() <= Instant.now().toEpochMilli());

    MockStartpointVisitor mockStartpointVisitorConsumer = new MockStartpointVisitor();
    startpoint.apply(new SystemStreamPartition("sys", "stream", new Partition(1)), mockStartpointVisitorConsumer);
    Assert.assertEquals(MockStartpointCustom.class, mockStartpointVisitorConsumer.visitedClass);
  }

  static class MockStartpointVisitor implements StartpointVisitor {
    Class<? extends Startpoint> visitedClass;

    @Override
    public void visit(SystemStreamPartition systemStreamPartition, StartpointSpecific startpointSpecific) {
      visitedClass = startpointSpecific.getClass();
    }

    @Override
    public void visit(SystemStreamPartition systemStreamPartition, StartpointTimestamp startpointTimestamp) {
      visitedClass = startpointTimestamp.getClass();
    }

    @Override
    public void visit(SystemStreamPartition systemStreamPartition, StartpointOldest startpointOldest) {
      visitedClass = startpointOldest.getClass();
    }

    @Override
    public void visit(SystemStreamPartition systemStreamPartition, StartpointUpcoming startpointUpcoming) {
      visitedClass = startpointUpcoming.getClass();
    }

    @Override
    public void visit(SystemStreamPartition systemStreamPartition, StartpointCustom startpointCustom) {
      visitedClass = startpointCustom.getClass();
    }
  }
}
