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

package org.apache.samza.config


import org.apache.samza.SamzaException

import scala.collection.JavaConversions._
import org.apache.samza.util.Logging
import org.apache.samza.util.Util

object StorageConfig {
  // stream config constants
  val FACTORY = "stores.%s.factory"
  val KEY_SERDE = "stores.%s.key.serde"
  val MSG_SERDE = "stores.%s.msg.serde"
  val CHANGELOG_STREAM = "stores.%s.changelog"
  val CHANGELOG_SYSTEM = "job.changelog.system"

  implicit def Config2Storage(config: Config) = new StorageConfig(config)
}

class StorageConfig(config: Config) extends ScalaMapConfig(config) with Logging {
  import StorageConfig._
  def getStorageFactoryClassName(name: String) = getOption(FACTORY.format(name))
  def getStorageKeySerde(name: String) = getOption(StorageConfig.KEY_SERDE format name)
  def getStorageMsgSerde(name: String) = getOption(StorageConfig.MSG_SERDE format name)
  def getChangelogStream(name: String) = {
    // If the config specifies 'stores.<storename>.changelog' as '<system>.<stream>' combination - it will take precedence.
    // If this config only specifies <astream> and there is a value in job.changelog.system=<asystem> -
    // these values will be combined into <asystem>.<astream>
    val systemStream = getOption(CHANGELOG_STREAM format name)
    val changelogSystem = getOption(CHANGELOG_SYSTEM)
    val systemStreamRes =
      if ( systemStream.isDefined  && ! systemStream.getOrElse("").contains('.')) {
        // contains only stream name
        if (changelogSystem.isDefined) {
          Some(changelogSystem.get + "." + systemStream.get)
        }
        else {
          throw new SamzaException("changelog system is not defined:" + systemStream.get)
        }
      } else {
        systemStream
      }
    systemStreamRes
  }

  def getStoreNames: Seq[String] = {
    val conf = config.subset("stores.", true)
    conf.keys.filter(k => k.endsWith(".factory")).map(k => k.substring(0, k.length - ".factory".length)).toSeq
  }

  /**
   * Helper method to check if a system has a changelog attached to it.
   */
  def isChangelogSystem(systemName: String) = {
    config
      .getStoreNames
      // Get changelogs for all stores in the format of "system.stream"
      .map(getChangelogStream(_))
      .filter(_.isDefined)
      // Convert "system.stream" to systemName
      .map(systemStreamName => Util.getSystemStreamFromNames(systemStreamName.get).getSystem)
      .contains(systemName)
  }
}
