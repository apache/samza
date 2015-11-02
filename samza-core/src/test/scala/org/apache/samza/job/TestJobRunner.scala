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

package org.apache.samza.job

import java.io.File

import org.apache.samza.SamzaException
import org.apache.samza.config.Config
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory
import org.apache.samza.migration.JobRunnerMigration
import org.junit.Test
import org.junit.After
import org.junit.Assert._

object TestJobRunner {
  var processCount = 0
}

class TestJobRunner {

  @After
  def teardown {
    MockCoordinatorStreamSystemFactory.disableMockConsumerCache()
  }

  @Test
  def testJobRunnerMigrationFails {
    MockCoordinatorStreamSystemFactory.enableMockConsumerCache()

    try {
      JobRunner.main(Array(
        "--config-factory",
        "org.apache.samza.config.factories.PropertiesConfigFactory",
        "--config-path",
        "file://%s/src/test/resources/test-migration-fail.properties" format new File(".").getCanonicalPath))
      fail("Should have failed already.")
    } catch {
      case se: SamzaException => assertEquals(se.getMessage, JobRunnerMigration.UNSUPPORTED_ERROR_MSG)
    }
  }

  @Test
  def testJobRunnerWorks {
    MockCoordinatorStreamSystemFactory.enableMockConsumerCache()

    JobRunner.main(Array(
      "--config-factory",
      "org.apache.samza.config.factories.PropertiesConfigFactory",
      "--config-path",
      "file://%s/src/test/resources/test.properties" format new File(".").getCanonicalPath))
    assertEquals(1, TestJobRunner.processCount)
  }
}

class MockJobFactory extends StreamJobFactory {
  def getJob(config: Config): StreamJob = {
    return new StreamJob {
      def submit() = { TestJobRunner.processCount += 1; this }
      def kill() = this
      def waitForFinish(timeoutMs: Long) = ApplicationStatus.SuccessfulFinish
      def waitForStatus(status: ApplicationStatus, timeoutMs: Long) = status
      def getStatus() = ApplicationStatus.SuccessfulFinish
    }
  }
}
