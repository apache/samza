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

package org.apache.samza.job.local;
import org.junit.Assert._
import org.junit.Test
import org.apache.samza.job.ApplicationStatus

class TestProcessJob {
  @Test
  def testProcessJobShouldFinishOnItsOwn {
    val builder = new ProcessBuilder("sleep", "1")
    val job = new ProcessJob(builder)
    job.submit
    job.waitForFinish(999999)
  }

  @Test
  def testProcessJobKillShouldWork {
    val builder = new ProcessBuilder("sleep", "999999999")
    val job = new ProcessJob(builder)
    job.submit
    job.waitForFinish(500)
    job.kill
    job.waitForFinish(999999)
    assert(ApplicationStatus.UnsuccessfulFinish.equals(job.waitForFinish(999999999)))
  }
}
