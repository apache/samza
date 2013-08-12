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
import org.apache.samza.config.Config
import org.junit.Assert._
import org.junit.Test

object TestJobRunner {
  var processCount = 0
}

class TestJobRunner {
  @Test
  def testJobRunnerWorks {
    JobRunner.main(Array(
      "--config-factory",
      "org.apache.samza.config.factories.PropertiesConfigFactory",
      "--config-path",
      "file://%s/src/test/resources/test.properties" format new File(".").getCanonicalPath))
    assert(TestJobRunner.processCount == 1)
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
