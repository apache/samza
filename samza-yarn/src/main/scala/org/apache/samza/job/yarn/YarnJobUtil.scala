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

import java.io.IOException

import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.samza.util.Logging

object JobContext {
  val STAGING_DIR = ".samzaStaging"
}

/**
  * JobContext is used to store the meta information about the job running on Yarn.
  *
  * Optionally, one can set Application Id and Application Staging Directory to the job context object.
  */
class JobContext {
  private var appId: ApplicationId = null
  private var appStagingDir: Path = null

  def getAppId: Option[ApplicationId] = {
    Option(appId)
  }

  def getAppStagingDir: Option[Path] = {
    Option(appStagingDir)
  }

  def setAppId(appId: ApplicationId) = {
    this.appId = appId
  }

  def setAppStagingDir(appStagingDir: Path) = {
    this.appStagingDir = appStagingDir
  }

  /**
    * Return the staging directory path to the given application.
    */
  def defaultAppStagingDir: Option[String] = {
    getAppId.map(JobContext.STAGING_DIR + Path.SEPARATOR + _.toString)
  }
}

object YarnJobUtil extends Logging {
  /**
    * Create the staging directory for the application.
    *
    * @param jobContext
    * @param fs
    * @return the Option of the Path object to the staging directory
    */
  def createStagingDir(jobContext: JobContext, fs: FileSystem) = {
    val defaultStagingDir = jobContext.defaultAppStagingDir.map(new Path(fs.getHomeDirectory, _))
    val stagingDir = jobContext.getAppStagingDir match {
      case appStagingDir: Some[Path] => appStagingDir
      case None => defaultStagingDir
  }
    stagingDir.map {
      appStagingDir =>
        val appStagingDirPermission = FsPermission.createImmutable(Integer.parseInt("700", 8).toShort)
        if (FileSystem.mkdirs(fs, appStagingDir, appStagingDirPermission)) {
          appStagingDir
        } else {
          null
        }
    }
  }

  /**
    * Clean up the application staging directory.
    */
  def cleanupStagingDir(jobContext: JobContext, fs: FileSystem): Unit = {
    jobContext.getAppStagingDir match {
      case Some(appStagingDir) => try {
        if (fs.exists(appStagingDir)) {
          info("Deleting staging directory " + appStagingDir)
          fs.delete(appStagingDir, true)
        }
      } catch {
        case ioe: IOException =>
          warn("Failed to cleanup staging dir " + appStagingDir, ioe)
      }
      case None => info("No staging dir exists")
    }
  }
}
