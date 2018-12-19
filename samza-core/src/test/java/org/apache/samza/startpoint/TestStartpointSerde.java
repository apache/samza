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

import org.junit.Assert;
import org.junit.Test;


public class TestStartpointSerde {
  private final StartpointSerde startpointSerde = new StartpointSerde();

  @Test
  public void testStartpointSpecificSerde() {
    StartpointSpecific startpointSpecific = new StartpointSpecific("42");
    Startpoint startpointFromSerde = startpointSerde.fromBytes(startpointSerde.toBytes(startpointSpecific));

    Assert.assertEquals(startpointSpecific.getClass(), startpointFromSerde.getClass());
    Assert.assertEquals(startpointSpecific.getCreationTimestamp(), startpointFromSerde.getCreationTimestamp());
    Assert.assertEquals(startpointSpecific.getSpecificOffset(), ((StartpointSpecific) startpointFromSerde).getSpecificOffset());
  }

  @Test
  public void testStartpointTimestampSerde() {
    StartpointTimestamp startpointTimestamp = new StartpointTimestamp(123456L);
    Startpoint startpointFromSerde = startpointSerde.fromBytes(startpointSerde.toBytes(startpointTimestamp));

    Assert.assertEquals(startpointTimestamp.getClass(), startpointFromSerde.getClass());
    Assert.assertEquals(startpointTimestamp.getCreationTimestamp(), startpointFromSerde.getCreationTimestamp());
    Assert.assertEquals(startpointTimestamp.getTimestampOffset(), ((StartpointTimestamp) startpointFromSerde).getTimestampOffset());
  }

  @Test
  public void testStartpointEarliestSerde() {
    StartpointOldest startpointOldest = new StartpointOldest();
    Startpoint startpointFromSerde = startpointSerde.fromBytes(startpointSerde.toBytes(startpointOldest));

    Assert.assertEquals(startpointOldest.getClass(), startpointFromSerde.getClass());
    Assert.assertEquals(startpointOldest.getCreationTimestamp(), startpointFromSerde.getCreationTimestamp());
  }

  @Test
  public void testStartpointLatestSerde() {
    StartpointUpcoming startpointUpcoming = new StartpointUpcoming();
    Startpoint startpointFromSerde = startpointSerde.fromBytes(startpointSerde.toBytes(startpointUpcoming));

    Assert.assertEquals(startpointUpcoming.getClass(), startpointFromSerde.getClass());
    Assert.assertEquals(startpointUpcoming.getCreationTimestamp(), startpointFromSerde.getCreationTimestamp());
  }

  @Test
  public void testStartpointCustomSerde() {
    MockStartpointCustom startpointCustom = new MockStartpointCustom("das boot", 42);
    Startpoint startpointFromSerde = startpointSerde.fromBytes(startpointSerde.toBytes(startpointCustom));

    Assert.assertEquals(startpointCustom.getClass(), startpointFromSerde.getClass());
    Assert.assertEquals(startpointCustom.getCreationTimestamp(), startpointFromSerde.getCreationTimestamp());
    Assert.assertEquals(startpointCustom.getTestInfo1(), ((MockStartpointCustom) startpointFromSerde).getTestInfo1());
    Assert.assertEquals(startpointCustom.getTestInfo2(), ((MockStartpointCustom) startpointFromSerde).getTestInfo2());
  }
}
