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
package org.apache.samza.job.yarn

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileStatus, Path, FileSystem}
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.samza.SamzaException
import org.apache.samza.config.{MapConfig, JobConfig, YarnConfig}
import org.mockito.Mockito._
import org.mockito.Matchers.any
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar


class TestClientHelper extends FunSuite {
  import MockitoSugar._
  val hadoopConfig = mock[Configuration]

  val clientHelper = new ClientHelper(hadoopConfig) {
    override def createYarnClient() = {
      mock[YarnClient]
    }
  }

  test("test validateJobConfig") {
    import collection.JavaConverters._
    var config = new MapConfig()

    intercept[SamzaException] {
      clientHelper.validateJobConfig(config)
    }

    config = new MapConfig(Map(JobConfig.JOB_SECURITY_MANAGER_FACTORY -> "some value").asJava)

    clientHelper.validateJobConfig(config)
  }

  test("test prepareJobConfig") {
    val jobContext = new JobContext
    jobContext.setAppStagingDir(new Path("/user/temp/.samzaStaging/app_123"))
    clientHelper.jobContext = jobContext

    val ret = clientHelper.getSecurityYarnConfig

    assert(ret.size == 2)
    assert(ret.get(YarnConfig.YARN_JOB_STAGING_DIRECTORY) == Some("/user/temp/.samzaStaging/app_123"))
    assert(ret.get(YarnConfig.YARN_CREDENTIALS_FILE) == Some("/user/temp/.samzaStaging/app_123/credentials"))
  }

  test("test setupAMLocalResources") {
    val applicationId = mock[ApplicationId]
    when(applicationId.toString).thenReturn("application_123")
    val jobContext = new JobContext
    jobContext.setAppId(applicationId)
    clientHelper.jobContext = jobContext

    val mockFs = mock[FileSystem]
    val fileStatus = new FileStatus(0, false, 0, 0, System.currentTimeMillis(), null)

    when(mockFs.getHomeDirectory).thenReturn(new Path("/user/test"))
    when(mockFs.getFileStatus(any[Path])).thenReturn(fileStatus)
    when(mockFs.mkdirs(any[Path])).thenReturn(true)

    doNothing().when(mockFs).copyFromLocalFile(any[Path], any[Path])
    doNothing().when(mockFs).setPermission(any[Path], any[FsPermission])

    val ret = clientHelper.setupAMLocalResources(mockFs, Some("some.principal"), Some("some.keytab"))

    assert(ret.size == 1)
    assert(ret.contains("some.keytab"))
  }
}
