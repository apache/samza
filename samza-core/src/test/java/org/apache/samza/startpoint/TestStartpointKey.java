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
import java.util.LinkedHashMap;
import org.apache.samza.Partition;
import org.apache.samza.container.TaskName;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.system.SystemStreamPartition;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;


public class TestStartpointKey {

  @Test
  public void testStartpointKey() {
    SystemStreamPartition ssp1 = new SystemStreamPartition("system", "stream", new Partition(2));
    SystemStreamPartition ssp2 = new SystemStreamPartition("system", "stream", new Partition(3));

    StartpointKey startpointKey1 = new StartpointKey(ssp1);
    StartpointKey startpointKey2 = new StartpointKey(ssp1);
    StartpointKey startpointKeyWithDifferentSSP = new StartpointKey(ssp2);
    StartpointKey startpointKeyWithTask1 = new StartpointKey(ssp1, new TaskName("t1"));
    StartpointKey startpointKeyWithTask2 = new StartpointKey(ssp1, new TaskName("t1"));
    StartpointKey startpointKeyWithDifferentTask = new StartpointKey(ssp1, new TaskName("t2"));

    Assert.assertEquals(startpointKey1, startpointKey2);
    Assert.assertEquals(new String(new JsonSerdeV2<>().toBytes(startpointKey1)),
        new String(new JsonSerdeV2<>().toBytes(startpointKey2)));
    Assert.assertEquals(startpointKeyWithTask1, startpointKeyWithTask2);
    Assert.assertEquals(new String(new JsonSerdeV2<>().toBytes(startpointKeyWithTask1)),
        new String(new JsonSerdeV2<>().toBytes(startpointKeyWithTask2)));

    Assert.assertNotEquals(startpointKey1, startpointKeyWithTask1);
    Assert.assertNotEquals(new String(new JsonSerdeV2<>().toBytes(startpointKey1)),
        new String(new JsonSerdeV2<>().toBytes(startpointKeyWithTask1)));

    Assert.assertNotEquals(startpointKey1, startpointKeyWithDifferentSSP);
    Assert.assertNotEquals(new String(new JsonSerdeV2<>().toBytes(startpointKey1)),
        new String(new JsonSerdeV2<>().toBytes(startpointKeyWithDifferentSSP)));
    Assert.assertNotEquals(startpointKeyWithTask1, startpointKeyWithDifferentTask);
    Assert.assertNotEquals(new String(new JsonSerdeV2<>().toBytes(startpointKeyWithTask1)),
        new String(new JsonSerdeV2<>().toBytes(startpointKeyWithDifferentTask)));

    Assert.assertNotEquals(startpointKeyWithTask1, startpointKeyWithDifferentTask);
    Assert.assertNotEquals(new String(new JsonSerdeV2<>().toBytes(startpointKeyWithTask1)),
        new String(new JsonSerdeV2<>().toBytes(startpointKeyWithDifferentTask)));
  }

  @Test
  public void testStartpointKeyFormat() throws IOException {
    SystemStreamPartition ssp = new SystemStreamPartition("system1", "stream1", new Partition(2));
    StartpointKey startpointKeyWithTask = new StartpointKey(ssp, new TaskName("t1"));
    ObjectMapper objectMapper = new ObjectMapper();
    byte[] jsonBytes = new JsonSerdeV2<>().toBytes(startpointKeyWithTask);
    LinkedHashMap<String, String> deserialized = objectMapper.readValue(jsonBytes, LinkedHashMap.class);

    Assert.assertEquals(4, deserialized.size());
    Assert.assertEquals("system1", deserialized.get("system"));
    Assert.assertEquals("stream1", deserialized.get("stream"));
    Assert.assertEquals(2, deserialized.get("partition"));
    Assert.assertEquals("t1", deserialized.get("taskName"));
  }
}
