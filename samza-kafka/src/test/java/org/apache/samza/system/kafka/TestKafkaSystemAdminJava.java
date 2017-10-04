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

import java.util.*;
import java.util.HashMap;
import java.util.Map;

import kafka.api.TopicMetadata;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.StreamValidationException;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.util.Util;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;


public class TestKafkaSystemAdminJava extends TestKafkaSystemAdmin {

  KafkaSystemAdmin basicSystemAdmin = createSystemAdmin();


  @Test
  public void testCreateCoordinatorStream() {
    KafkaSystemAdmin systemAdmin = createSystemAdmin();//coordProps, 3, new scala.collection.immutable.HashMap<>(), 1000);
    SystemAdmin admin = Mockito.spy(systemAdmin);
    StreamSpec spec = StreamSpec.createCoordinatorStreamSpec("testCoordinatorStream", "testSystem");

    admin.createStream(spec);
    admin.validateStream(spec);

    Mockito.verify(admin).createStream(Mockito.any());
  }

  @Test
  public void testCreateCoordinatorStreamWithSpecialCharsInTopicName() {
    final String STREAM = "test.coordinator_test.Stream";

    Properties coordProps = new Properties();
    Map<String, ChangelogInfo> changeLogMap = new HashMap<>();

    KafkaSystemAdmin admin = Mockito.spy(createSystemAdmin(coordProps, 1, Util.javaMapAsScalaMap(changeLogMap)));
    StreamSpec spec = StreamSpec.createCoordinatorStreamSpec(STREAM, SYSTEM());

    Mockito.doAnswer(invocationOnMock -> {
      StreamSpec internalSpec = (StreamSpec) invocationOnMock.callRealMethod();
      assertTrue(internalSpec instanceof KafkaStreamSpec);  // KafkaStreamSpec is used to carry replication factor
      assertTrue(internalSpec.isCoordinatorStream());
      assertEquals(SYSTEM(), internalSpec.getSystemName());
      assertEquals(STREAM, internalSpec.getPhysicalName());
      assertEquals(1, internalSpec.getPartitionCount());

      return internalSpec;
    }).when(admin).toKafkaSpec(Mockito.any());

    admin.createStream(spec);
    admin.validateStream(spec);

  }

  @Test
  public void testCreateChangelogStream() {
    final String STREAM = "testChangeLogStream";
    final int PARTITIONS = 12;
    final int REP_FACTOR = 1;

    Properties coordProps = new Properties();
    Properties changeLogProps = new Properties();
    changeLogProps.setProperty("cleanup.policy", "compact");
    changeLogProps.setProperty("segment.bytes", "139");
    Map<String, ChangelogInfo> changeLogMap = new HashMap<>();
    changeLogMap.put(STREAM, new ChangelogInfo(REP_FACTOR, changeLogProps));

    KafkaSystemAdmin admin = Mockito.spy(createSystemAdmin(coordProps, 1, Util.javaMapAsScalaMap(changeLogMap)));
    StreamSpec spec = StreamSpec.createChangeLogStreamSpec(STREAM, SYSTEM(), PARTITIONS);

    Mockito.doAnswer(invocationOnMock -> {
      StreamSpec internalSpec = (StreamSpec) invocationOnMock.callRealMethod();
      assertTrue(internalSpec instanceof KafkaStreamSpec);  // KafkaStreamSpec is used to carry replication factor
      assertTrue(internalSpec.isChangeLogStream());
      assertEquals(SYSTEM(), internalSpec.getSystemName());
      assertEquals(STREAM, internalSpec.getPhysicalName());
      assertEquals(REP_FACTOR, ((KafkaStreamSpec) internalSpec).getReplicationFactor());
      assertEquals(PARTITIONS, internalSpec.getPartitionCount());
      assertEquals(changeLogProps, ((KafkaStreamSpec) internalSpec).getProperties());

      return internalSpec;
    }).when(admin).toKafkaSpec(Mockito.any());

    admin.createStream(spec);
    admin.validateStream(spec);
  }

  @Test
  public void testCreateChangelogStreamWithSpecialCharsInTopicName() {
    final String STREAM = "test.Change_Log.Stream";
    final int PARTITIONS = 12;
    final int REP_FACTOR = 1;

    Properties coordProps = new Properties();
    Properties changeLogProps = new Properties();
    changeLogProps.setProperty("cleanup.policy", "compact");
    changeLogProps.setProperty("segment.bytes", "139");
    Map<String, ChangelogInfo> changeLogMap = new HashMap<>();
    changeLogMap.put(STREAM, new ChangelogInfo(REP_FACTOR, changeLogProps));

    KafkaSystemAdmin admin = Mockito.spy(createSystemAdmin(coordProps, 1, Util.javaMapAsScalaMap(changeLogMap)));
    StreamSpec spec = StreamSpec.createChangeLogStreamSpec(STREAM, SYSTEM(), PARTITIONS);
    Mockito.doAnswer(invocationOnMock -> {
      StreamSpec internalSpec = (StreamSpec) invocationOnMock.callRealMethod();
      assertTrue(internalSpec instanceof KafkaStreamSpec);  // KafkaStreamSpec is used to carry replication factor
      assertTrue(internalSpec.isChangeLogStream());
      assertEquals(SYSTEM(), internalSpec.getSystemName());
      assertEquals(STREAM, internalSpec.getPhysicalName());
      assertEquals(REP_FACTOR, ((KafkaStreamSpec) internalSpec).getReplicationFactor());
      assertEquals(PARTITIONS, internalSpec.getPartitionCount());
      assertEquals(changeLogProps, ((KafkaStreamSpec) internalSpec).getProperties());

      return internalSpec;
    }).when(admin).toKafkaSpec(Mockito.any());

    admin.createStream(spec);
    admin.validateStream(spec);
  }

  @Test
  public void testCreateStream() {
    SystemAdmin admin = this.basicSystemAdmin;
    StreamSpec spec = new StreamSpec("testId", "testStream", "testSystem", 8);

    assertTrue("createStream should return true if the stream does not exist and then is created.", admin.createStream(spec));
    admin.validateStream(spec);

    assertFalse("createStream should return false if the stream already exists.", admin.createStream(spec));
  }

  @Test(expected = StreamValidationException.class)
  public void testValidateStreamDoesNotExist() {
    SystemAdmin admin = this.basicSystemAdmin;

    StreamSpec spec = new StreamSpec("testId", "testStreamNameExist", "testSystem", 8);

    admin.validateStream(spec);
  }

  @Test(expected = StreamValidationException.class)
  public void testValidateStreamWrongPartitionCount() {
    SystemAdmin admin = this.basicSystemAdmin;
    StreamSpec spec1 = new StreamSpec("testId", "testStreamPartition", "testSystem", 8);
    StreamSpec spec2 = new StreamSpec("testId", "testStreamPartition", "testSystem", 4);

    assertTrue("createStream should return true if the stream does not exist and then is created.", admin.createStream(spec1));

    admin.validateStream(spec2);
  }

  @Test(expected = StreamValidationException.class)
  public void testValidateStreamWrongName() {
    SystemAdmin admin = this.basicSystemAdmin;
    StreamSpec spec1 = new StreamSpec("testId", "testStreamName1", "testSystem", 8);
    StreamSpec spec2 = new StreamSpec("testId", "testStreamName2", "testSystem", 8);

    assertTrue("createStream should return true if the stream does not exist and then is created.", admin.createStream(spec1));

    admin.validateStream(spec2);
  }

  @Test
  public void testClearStream() {
    KafkaSystemAdmin admin = this.basicSystemAdmin;
    StreamSpec spec = new StreamSpec("testId", "testStreamClear", "testSystem", 8);

    assertTrue("createStream should return true if the stream does not exist and then is created.", admin.createStream(spec));
    assertTrue(admin.clearStream(spec));

    scala.collection.immutable.Set<String> topic = new scala.collection.immutable.Set.Set1<>(spec.getPhysicalName());
    scala.collection.immutable.Map<String, TopicMetadata> metadata = admin.getTopicMetadata(topic);
    assertTrue(metadata.get(spec.getPhysicalName()).get().partitionsMetadata().isEmpty());
  }
}
