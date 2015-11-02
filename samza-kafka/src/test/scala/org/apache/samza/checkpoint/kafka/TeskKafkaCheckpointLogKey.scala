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
package org.apache.samza.checkpoint.kafka

import org.apache.samza.SamzaException
import org.apache.samza.container.TaskName
import org.junit.Assert._
import org.junit.{Before, Test}

class TestKafkaCheckpointLogKey {
  @Before
  def setSSPGrouperFactoryString() {
    KafkaCheckpointLogKey.setSystemStreamPartitionGrouperFactoryString("hello")
  }

  @Test
  def checkpointKeySerializationRoundTrip() {
    val checkpointKey = KafkaCheckpointLogKey.getCheckpointKey(new TaskName("TN"))
    val asBytes = checkpointKey.toBytes()
    val backFromBytes = KafkaCheckpointLogKey.fromBytes(asBytes)

    assertEquals(checkpointKey, backFromBytes)
    assertNotSame(checkpointKey, backFromBytes)
  }

  @Test
  def changelogPartitionMappingKeySerializationRoundTrip() {
    val key = KafkaCheckpointLogKey.getChangelogPartitionMappingKey()
    val asBytes = key.toBytes()
    val backFromBytes = KafkaCheckpointLogKey.fromBytes(asBytes)

    assertEquals(key, backFromBytes)
    assertNotSame(key, backFromBytes)
  }

  @Test
  def differingSSPGrouperFactoriesCauseException() {

    val checkpointKey = KafkaCheckpointLogKey.getCheckpointKey(new TaskName("TN"))

    val asBytes = checkpointKey.toBytes()

    KafkaCheckpointLogKey.setSystemStreamPartitionGrouperFactoryString("goodbye")

    var gotException = false
    try {
      KafkaCheckpointLogKey.fromBytes(asBytes)
    } catch {
      case se:SamzaException => assertEquals(new DifferingSystemStreamPartitionGrouperFactoryValues("hello", "goodbye").getMessage(), se.getCause.getMessage)
        gotException = true
    }

    assertTrue("Should have had an exception since ssp grouper factories didn't match", gotException)
  }
}