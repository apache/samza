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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.StreamValidationException;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.util.Util;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.Assert.*;


public class TestKafkaSystemAdminJava extends TestKafkaSystemAdmin {

  KafkaSystemAdmin basicSystemAdmin = createSystemAdmin();


  @Test
  public void testCreateCoordinatorStreamDelegatesToCreateStream() {
    KafkaSystemAdmin systemAdmin = createSystemAdmin();//coordProps, 3, new scala.collection.immutable.HashMap<>(), 1000);
    SystemAdmin admin = Mockito.spy(systemAdmin);
    StreamSpec spec = new StreamSpec("testId", "testCoordinatorStream", "testSystem");

    admin.createCoordinatorStream(spec.getPhysicalName());
    admin.validateStream(spec);

    Mockito.verify(admin).createStream(Mockito.any());
  }

  @Test
  public void testCreateChangelogStreamDelegatesToCreateStream() {
    final String STREAM = "testChangeLogStream";
    final int PARTITIONS = 12;
    final int REP_FACTOR = 3;

    Properties coordProps = new Properties();
    Properties changeLogProps = new Properties();
    changeLogProps.setProperty("cleanup.policy", "compact");
    changeLogProps.setProperty("segment.bytes", "139");
    Map<String, ChangelogInfo> changeLogMap = new HashMap<>();
    changeLogMap.put(STREAM, new ChangelogInfo(REP_FACTOR, changeLogProps));

    SystemAdmin admin = Mockito.spy(createSystemAdmin(coordProps, 3, Util.javaMapAsScalaMap(changeLogMap)));
    StreamSpec spec = new StreamSpec(KafkaSystemAdmin.CHANGELOG_STREAMID(), STREAM, SYSTEM(), PARTITIONS);
    admin.createChangelogStream(STREAM, PARTITIONS);
    admin.validateStream(spec);

    ArgumentCaptor<StreamSpec> specCaptor = ArgumentCaptor.forClass(StreamSpec.class);
    Mockito.verify(admin).createStream(specCaptor.capture());

    StreamSpec internalSpec = specCaptor.getValue();
    assertTrue(internalSpec instanceof KafkaStreamSpec);  // KafkaStreamSpec is used to carry replication factor
    assertEquals(KafkaSystemAdmin.CHANGELOG_STREAMID(), internalSpec.getId());
    assertEquals(SYSTEM(), internalSpec.getSystemName());
    assertEquals(STREAM, internalSpec.getPhysicalName());
    assertEquals(REP_FACTOR, ((KafkaStreamSpec) internalSpec).getReplicationFactor());
    assertEquals(PARTITIONS, internalSpec.getPartitionCount());
    assertEquals(changeLogProps, ((KafkaStreamSpec) internalSpec).getProperties());
  }

  @Test
  public void testCreateChangelogStreamDelegatesToCreateStream_specialCharsInTopicName() {
    final String STREAM = "test.Change_Log.Stream";
    final int PARTITIONS = 12;
    final int REP_FACTOR = 3;

    Properties coordProps = new Properties();
    Properties changeLogProps = new Properties();
    changeLogProps.setProperty("cleanup.policy", "compact");
    changeLogProps.setProperty("segment.bytes", "139");
    Map<String, ChangelogInfo> changeLogMap = new HashMap<>();
    changeLogMap.put(STREAM, new ChangelogInfo(REP_FACTOR, changeLogProps));

    SystemAdmin admin = Mockito.spy(createSystemAdmin(coordProps, 3, Util.javaMapAsScalaMap(changeLogMap)));
    StreamSpec spec = new StreamSpec(KafkaSystemAdmin.CHANGELOG_STREAMID(), STREAM, SYSTEM(), PARTITIONS);
    admin.createChangelogStream(STREAM, PARTITIONS);
    admin.validateStream(spec);

    ArgumentCaptor<StreamSpec> specCaptor = ArgumentCaptor.forClass(StreamSpec.class);
    Mockito.verify(admin).createStream(specCaptor.capture());

    StreamSpec internalSpec = specCaptor.getValue();
    assertTrue(internalSpec instanceof KafkaStreamSpec);  // KafkaStreamSpec is used to carry replication factor
    assertEquals(KafkaSystemAdmin.CHANGELOG_STREAMID(), internalSpec.getId());
    assertEquals(SYSTEM(), internalSpec.getSystemName());
    assertEquals(STREAM, internalSpec.getPhysicalName());
    assertEquals(REP_FACTOR, ((KafkaStreamSpec) internalSpec).getReplicationFactor());
    assertEquals(PARTITIONS, internalSpec.getPartitionCount());
    assertEquals(changeLogProps, ((KafkaStreamSpec) internalSpec).getProperties());
  }

  @Test
  public void testValidateChangelogStreamDelegatesToValidateStream() {
    final String STREAM = "testChangeLogValidate";
    Properties coordProps = new Properties();
    Map<String, ChangelogInfo> changeLogMap = new HashMap<>();
    changeLogMap.put(STREAM, new ChangelogInfo(3, new Properties()));

    KafkaSystemAdmin systemAdmin = createSystemAdmin(coordProps, 3, Util.javaMapAsScalaMap(changeLogMap));
    SystemAdmin admin = Mockito.spy(systemAdmin);
    StreamSpec spec = new StreamSpec("testId", STREAM, "testSystem", 12);

    admin.createChangelogStream(spec.getPhysicalName(), spec.getPartitionCount());
    admin.validateStream(spec);
    admin.validateChangelogStream(spec.getPhysicalName(), spec.getPartitionCount());

    Mockito.verify(admin).createStream(Mockito.any());
    Mockito.verify(admin, Mockito.times(3)).validateStream(Mockito.any());
  }

  @Test
  public void testValidateChangelogStreamDelegatesToCreateStream_specialCharsInTopicName() {
    final String STREAM = "test.Change_Log.Validate";
    Properties coordProps = new Properties();
    Map<String, ChangelogInfo> changeLogMap = new HashMap<>();
    changeLogMap.put(STREAM, new ChangelogInfo(3, new Properties()));

    KafkaSystemAdmin systemAdmin = createSystemAdmin(coordProps, 3, Util.javaMapAsScalaMap(changeLogMap));
    SystemAdmin admin = Mockito.spy(systemAdmin);
    StreamSpec spec = new StreamSpec("testId", STREAM, "testSystem", 12);

    admin.createChangelogStream(spec.getPhysicalName(), spec.getPartitionCount());
    admin.validateStream(spec);
    admin.validateChangelogStream(STREAM, spec.getPartitionCount()); // Should not throw

    Mockito.verify(admin).createStream(Mockito.any());
    Mockito.verify(admin, Mockito.times(3)).validateStream(Mockito.any());
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
}
