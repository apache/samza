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

import java.security.PrivilegedExceptionAction
import java.util.concurrent.{TimeUnit, Executors}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.samza.SamzaException
import org.apache.samza.config.{YarnConfig, Config}
import org.apache.samza.util.{DaemonThreadFactory, Logging}
import org.apache.samza.container.SecurityManager

object SamzaAppMasterSecurityManager {
  val TOKEN_RENEW_THREAD_NAME_PREFIX = "TOKEN-RENEW-PREFIX"
}

/**
  * The SamzaAppMasterSecurityManager is responsible for renewing and distributing HDFS delegation tokens on a secure YARN
  * cluster.
  *
  * <p />
  *
  * It runs in a daemon thread and periodically requests new delegation tokens and writes the fresh tokens in a configured
  * staging directory at the configured frequency.
  *
  * @param config     Samza config for the application
  * @param hadoopConf the hadoop configuration
  */
class SamzaAppMasterSecurityManager(config: Config, hadoopConf: Configuration) extends SecurityManager with Logging {
  private val tokenRenewExecutor = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory(SamzaAppMasterSecurityManager
    .TOKEN_RENEW_THREAD_NAME_PREFIX))

  def start() = {
    val yarnConfig = new YarnConfig(config)
    val principal = yarnConfig.getYarnKerberosPrincipal
    // only get the name part of the keytab config, the keytab file will in the working directory
    val keytab = new Path(yarnConfig.getYarnKerberosKeytab).getName
    val renewalInterval = yarnConfig.getYarnTokenRenewalIntervalSeconds
    val credentialsFile = yarnConfig.getYarnCredentialsFile

    val tokenRenewRunnable = new Runnable {
      override def run(): Unit = {
        try {
          loginFromKeytab(principal, keytab, credentialsFile)
        } catch {
          case e: Exception =>
            warn("Failed to renew token and write out new credentials", e)
        }
      }
    }

    tokenRenewExecutor.scheduleAtFixedRate(tokenRenewRunnable, renewalInterval, renewalInterval, TimeUnit.SECONDS)
  }

  private def loginFromKeytab(principal: String, keytab: String, credentialsFile: String) = {
    info(s"Logging to KDC using principal: $principal")
    val keytabUser = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab)
    val credentials = keytabUser.getCredentials

    // get the new delegation token from the key tab user
    keytabUser.doAs(new PrivilegedExceptionAction[Void] {
      override def run(): Void = {
        getNewDelegationToken(credentials)
        null
      }
    })

    UserGroupInformation.getCurrentUser.addCredentials(credentials)

    val credentialsFilePath = new Path(credentialsFile)
    writeNewDelegationToken(credentialsFilePath, credentials)
  }

  private def getNewDelegationToken(credentials: Credentials) = {
    val fs = FileSystem.get(hadoopConf)
    val tokenRenewer = UserGroupInformation.getCurrentUser.getShortUserName
    // HDFS will not issue new delegation token if there are existing ones in the credentials. This is hacked
    // by creating a new credentials object from the login via the given keytab and principle, passing the new
    // credentials object to FileSystem.addDelegationTokens to force a new delegation token created and adding
    // the credentials to the current user's credential object
    fs.addDelegationTokens(tokenRenewer, credentials)
  }

  private def writeNewDelegationToken(credentialsFilePath: Path, credentials: Credentials) = {
    val fs = FileSystem.get(hadoopConf)
    if (fs.exists(credentialsFilePath)) {
      logger.info(s"Deleting existing credentials file $credentialsFilePath")
      val success = fs.delete(credentialsFilePath, false)
      if (!success) {
        throw new SamzaException(s"Failed deleting existing credentials file $credentialsFilePath")
      }
    }

    logger.info(s"Writing new delegation to the token file $credentialsFilePath")
    credentials.writeTokenStorageFile(credentialsFilePath, hadoopConf)
  }

  def stop() = {
    tokenRenewExecutor.shutdown
  }
}
