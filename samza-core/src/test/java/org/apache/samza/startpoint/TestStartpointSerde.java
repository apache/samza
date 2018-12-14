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

  public TestStartpointSerde() {
    // Register each Startpoint type with the serde
    startpointSerde.register(StartpointSpecific.class);
    startpointSerde.register(StartpointTimestamp.class);
    startpointSerde.register(StartpointEarliest.class);
    startpointSerde.register(StartpointLatest.class);
    startpointSerde.register(StartpointBootstrap.class);
  }

  @Test
  public void testStartpointSpecificSerde() {
    StartpointSpecific startpointSpecific = new StartpointSpecific("42");
    Startpoint startpointFromSerde = startpointSerde.fromBytes(startpointSerde.toBytes(startpointSpecific));

    Assert.assertEquals(startpointSpecific.getClass(), startpointFromSerde.getClass());
    Assert.assertEquals(startpointSpecific.getCreatedTimestamp(), startpointFromSerde.getCreatedTimestamp());
    Assert.assertEquals(startpointSpecific.getSpecificOffset(), ((StartpointSpecific) startpointFromSerde).getSpecificOffset());
  }

  @Test
  public void testStartpointTimestampSerde() {
    StartpointTimestamp startpointTimestamp = new StartpointTimestamp(123456L);
    Startpoint startpointFromSerde = startpointSerde.fromBytes(startpointSerde.toBytes(startpointTimestamp));

    Assert.assertEquals(startpointTimestamp.getClass(), startpointFromSerde.getClass());
    Assert.assertEquals(startpointTimestamp.getCreatedTimestamp(), startpointFromSerde.getCreatedTimestamp());
    Assert.assertEquals(startpointTimestamp.getTimestampOffset(), ((StartpointTimestamp) startpointFromSerde).getTimestampOffset());
  }

  @Test
  public void testStartpointEarliestSerde() {
    StartpointEarliest startpointEarliest = new StartpointEarliest();
    Startpoint startpointFromSerde = startpointSerde.fromBytes(startpointSerde.toBytes(startpointEarliest));

    Assert.assertEquals(startpointEarliest.getClass(), startpointFromSerde.getClass());
    Assert.assertEquals(startpointEarliest.getCreatedTimestamp(), startpointFromSerde.getCreatedTimestamp());
  }

  @Test
  public void testStartpointLatestSerde() {
    StartpointLatest startpointLatest = new StartpointLatest();
    Startpoint startpointFromSerde = startpointSerde.fromBytes(startpointSerde.toBytes(startpointLatest));

    Assert.assertEquals(startpointLatest.getClass(), startpointFromSerde.getClass());
    Assert.assertEquals(startpointLatest.getCreatedTimestamp(), startpointFromSerde.getCreatedTimestamp());
  }

  @Test
  public void testStartpointBootstrapSerde() {
    StartpointBootstrap startpointBootstrap = new StartpointBootstrap("das boot");
    Startpoint startpointFromSerde = startpointSerde.fromBytes(startpointSerde.toBytes(startpointBootstrap));

    Assert.assertEquals(startpointBootstrap.getClass(), startpointFromSerde.getClass());
    Assert.assertEquals(startpointBootstrap.getCreatedTimestamp(), startpointFromSerde.getCreatedTimestamp());
    Assert.assertEquals(startpointBootstrap.getBootstrapInfo(), ((StartpointBootstrap) startpointFromSerde).getBootstrapInfo());
  }
}
