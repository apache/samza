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

import java.io.IOException;
import java.time.Instant;
import org.apache.samza.Partition;
import org.apache.samza.system.SystemStreamPartition;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class TestStartpointObjectMapper {
  private static final ObjectMapper MAPPER = StartpointObjectMapper.getObjectMapper();

  @Test
  public void testStartpointSpecificSerde() throws IOException {
    StartpointSpecific startpointSpecific = new StartpointSpecific("42");
    Startpoint startpointFromSerde = MAPPER.readValue(MAPPER.writeValueAsBytes(startpointSpecific), Startpoint.class);

    Assert.assertEquals(startpointSpecific.getClass(), startpointFromSerde.getClass());
    Assert.assertEquals(startpointSpecific.getCreationTimestamp(), startpointFromSerde.getCreationTimestamp());
    Assert.assertEquals(startpointSpecific.getSpecificOffset(), ((StartpointSpecific) startpointFromSerde).getSpecificOffset());
  }

  @Test
  public void testStartpointTimestampSerde() throws IOException {
    StartpointTimestamp startpointTimestamp = new StartpointTimestamp(123456L);
    Startpoint startpointFromSerde = MAPPER.readValue(MAPPER.writeValueAsBytes(startpointTimestamp), Startpoint.class);

    Assert.assertEquals(startpointTimestamp.getClass(), startpointFromSerde.getClass());
    Assert.assertEquals(startpointTimestamp.getCreationTimestamp(), startpointFromSerde.getCreationTimestamp());
    Assert.assertEquals(startpointTimestamp.getTimestampOffset(), ((StartpointTimestamp) startpointFromSerde).getTimestampOffset());
  }

  @Test
  public void testStartpointEarliestSerde() throws IOException {
    StartpointOldest startpointOldest = new StartpointOldest();
    Startpoint startpointFromSerde = MAPPER.readValue(MAPPER.writeValueAsBytes(startpointOldest), Startpoint.class);

    Assert.assertEquals(startpointOldest.getClass(), startpointFromSerde.getClass());
    Assert.assertEquals(startpointOldest.getCreationTimestamp(), startpointFromSerde.getCreationTimestamp());
  }

  @Test
  public void testStartpointLatestSerde() throws IOException {
    StartpointUpcoming startpointUpcoming = new StartpointUpcoming();
    Startpoint startpointFromSerde = MAPPER.readValue(MAPPER.writeValueAsBytes(startpointUpcoming), Startpoint.class);

    Assert.assertEquals(startpointUpcoming.getClass(), startpointFromSerde.getClass());
    Assert.assertEquals(startpointUpcoming.getCreationTimestamp(), startpointFromSerde.getCreationTimestamp());
  }

  @Test
  public void testFanOutSerde() throws IOException {
    StartpointFanOutPerTask startpointFanOutPerTask = new StartpointFanOutPerTask(Instant.now().minusSeconds(60));
    startpointFanOutPerTask.getFanOuts()
        .put(new SystemStreamPartition("system1", "stream1", new Partition(1)), new StartpointUpcoming());
    startpointFanOutPerTask.getFanOuts()
        .put(new SystemStreamPartition("system2", "stream2", new Partition(2)), new StartpointOldest());

    String serialized = MAPPER.writeValueAsString(startpointFanOutPerTask);
    StartpointFanOutPerTask startpointFanOutPerTaskFromSerde = MAPPER.readValue(serialized, StartpointFanOutPerTask.class);

    Assert.assertEquals(startpointFanOutPerTask, startpointFanOutPerTaskFromSerde);
  }
}
