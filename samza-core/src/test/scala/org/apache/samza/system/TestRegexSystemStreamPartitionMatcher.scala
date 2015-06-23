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
package org.apache.samza.system

import java.util

import org.apache.samza.{Partition, SamzaException}
import org.apache.samza.config._
import org.apache.samza.coordinator.{JobCoordinator, MockCheckpointManagerFactory, MockSystemFactory}
import org.apache.samza.job.local.ThreadJobFactory
import org.junit.rules.ExpectedException
import org.junit.{Rule, Test}
import org.junit.Assert._

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

class TestRegexSystemStreamPartitionMatcher {
  val sspSet = mutable.Set(new SystemStreamPartition("test", "stream1", new Partition(0)))
  sspSet.add(new SystemStreamPartition("test", "stream1", new Partition(1)))
  sspSet.add(new SystemStreamPartition("test", "stream1", new Partition(2)))

  @Test
  def testFilterWithMatcherConfigRegex() {
    val config = new MapConfig(mutable.Map(
      TaskConfig.CHECKPOINT_MANAGER_FACTORY -> classOf[MockCheckpointManagerFactory].getCanonicalName,
      TaskConfig.INPUT_STREAMS -> "test.stream1",
      JobConfig.STREAM_JOB_FACTORY_CLASS -> classOf[ThreadJobFactory].getCanonicalName,
      JobConfig.SSP_MATCHER_CLASS -> JobConfig.DEFAULT_SSP_MATCHER_CLASS,
      JobConfig.SSP_MATCHER_CONFIG_REGEX -> "[1-2]",
      (SystemConfig.SYSTEM_FACTORY format "test") -> classOf[MockSystemFactory].getCanonicalName))

    val expectedSspSet = mutable.Set(new SystemStreamPartition("test", "stream1", new Partition(1)))
    expectedSspSet.add(new SystemStreamPartition("test", "stream1", new Partition(2)))

    val filteredSet = new RegexSystemStreamPartitionMatcher().filter(sspSet.asJava, config)
    assertEquals(2, filteredSet.size)
    assertEquals(expectedSspSet.asJava, filteredSet)
  }

  @Test(expected = classOf[SamzaException])
  def testFilterWithNoMatcherConfigRegex() {
    val config = new MapConfig(mutable.Map(
      TaskConfig.CHECKPOINT_MANAGER_FACTORY -> classOf[MockCheckpointManagerFactory].getCanonicalName,
      TaskConfig.INPUT_STREAMS -> "test.stream1",
      JobConfig.STREAM_JOB_FACTORY_CLASS -> classOf[ThreadJobFactory].getCanonicalName,
      JobConfig.SSP_MATCHER_CLASS -> JobConfig.DEFAULT_SSP_MATCHER_CLASS,
      (SystemConfig.SYSTEM_FACTORY format "test") -> classOf[MockSystemFactory].getCanonicalName))

    new RegexSystemStreamPartitionMatcher().filter(sspSet, config)
  }

  @Test
  def testFilterWithMatcherConfigRegexWithNomatches() {
    val config = new MapConfig(mutable.Map(
      TaskConfig.CHECKPOINT_MANAGER_FACTORY -> classOf[MockCheckpointManagerFactory].getCanonicalName,
      TaskConfig.INPUT_STREAMS -> "test.stream1",
      JobConfig.STREAM_JOB_FACTORY_CLASS -> classOf[ThreadJobFactory].getCanonicalName,
      JobConfig.SSP_MATCHER_CLASS -> JobConfig.DEFAULT_SSP_MATCHER_CLASS,
      JobConfig.SSP_MATCHER_CONFIG_REGEX -> "--",
      (SystemConfig.SYSTEM_FACTORY format "test") -> classOf[MockSystemFactory].getCanonicalName))

    val filteredSet = new RegexSystemStreamPartitionMatcher().filter(sspSet, config)
    assertEquals(0, filteredSet.size)
  }

}