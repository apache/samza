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

import java.util.concurrent.{Executors, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.yarn.api.records.{ApplicationAccessType, ContainerLaunchContext}
import org.apache.samza.config.{Config, YarnConfig}
import org.apache.samza.container.SecurityManager
import org.apache.samza.util.Logging

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap

class SamzaContainerSecurityManager(config: Config, hadoopConfig: Configuration) extends SecurityManager with Logging {
  private val InitialDelayInSeconds = 60

  private val tokenRenewExecutor = Executors.newSingleThreadScheduledExecutor(
    new ThreadFactoryBuilder()
      .setNameFormat("Samza ContainerSecurityManager TokenRenewer Thread-%d")
      .setDaemon(true)
      .build())

  private var lastRefreshTimestamp = 0L

  def setApplicationAcl(ctx: ContainerLaunchContext) = {
    val yarnConfig = new YarnConfig(config)
    val viewAcl = yarnConfig.getYarnApplicationViewAcl
    val modifyAcl = yarnConfig.getYarnApplicationModifyAcl
    val acls: HashMap[ApplicationAccessType, String] = HashMap[ApplicationAccessType, String]()
    if (viewAcl != null) {
      acls += ApplicationAccessType.VIEW_APP -> viewAcl
    }
    if (modifyAcl != null) {
      acls += ApplicationAccessType.MODIFY_APP -> modifyAcl
    }
    if (acls.nonEmpty) {
      info("setting application acls in launch context: %s" format acls)
      ctx.setApplicationACLs(acls.asJava)
    }
  }

  def start() = {
    val yarnConfig = new YarnConfig(config)
    val renewalInterval = yarnConfig.getYarnTokenRenewalIntervalSeconds
    val tokenFilePath = new Path(yarnConfig.getYarnCredentialsFile)

    val tokenRenewRunnable = new Runnable {
      override def run(): Unit = {
        try {
          val fs = FileSystem.get(hadoopConfig)
          if (fs.exists(tokenFilePath)) {
            val fileStatus = fs.getFileStatus(tokenFilePath)
            if (lastRefreshTimestamp > fileStatus.getModificationTime) {
              // credentials have not been updated, retry after 5 minutes
              info("Expecting to update delegation tokens, but AM has not updated credentials file yet, will retry in 5 minutes")
              tokenRenewExecutor.schedule(this, 5, TimeUnit.MINUTES)
            } else {
              val credentials = getCredentialsFromHDFS(fs, tokenFilePath)
              UserGroupInformation.getCurrentUser.addCredentials(credentials)
              info("Successfully renewed tokens from credentials file")
              lastRefreshTimestamp = System.currentTimeMillis
              info(s"Schedule the next fetch in $renewalInterval seconds")
              tokenRenewExecutor.schedule(this, renewalInterval, TimeUnit.SECONDS)
            }
          } else {
            info(s"Credentials file not found yet. Schedule the next fetch in $renewalInterval seconds")
            tokenRenewExecutor.schedule(this, renewalInterval, TimeUnit.SECONDS)
          }
        } catch {
          case e: Exception =>
            val retrySeconds = Math.min(renewalInterval, 3600)
            warn(s"Failed to renew tokens, will retry in $retrySeconds seconds", e)

            tokenRenewExecutor.schedule(this, retrySeconds, TimeUnit.SECONDS)
        }
      }
    }

    info(s"Schedule the next fetch in ${renewalInterval + InitialDelayInSeconds} seconds")
    tokenRenewExecutor.schedule(tokenRenewRunnable, renewalInterval + InitialDelayInSeconds, TimeUnit.SECONDS)
  }

  private def getCredentialsFromHDFS(fs: FileSystem, tokenPath: Path): Credentials = {
    val stream = fs.open(tokenPath)
    try {
      val newCredentials = new Credentials()
      newCredentials.readTokenStorageStream(stream)
      newCredentials
    } finally {
      stream.close()
    }
  }

  def stop(): Unit = {
    tokenRenewExecutor.shutdown()
  }
}
