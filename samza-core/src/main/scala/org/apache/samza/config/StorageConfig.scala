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

import scala.collection.JavaConversions._

import grizzled.slf4j.Logging

object StorageConfig {
  // stream config constants
  val FACTORY = "stores.%s.factory"
  val KEY_SERDE = "stores.%s.key.serde"
  val MSG_SERDE = "stores.%s.msg.serde"
  val CHANGELOG_STREAM = "stores.%s.changelog"

  implicit def Config2Storage(config: Config) = new StorageConfig(config)
}

class StorageConfig(config: Config) extends ScalaMapConfig(config) with Logging {
  import StorageConfig._
  def getStorageFactoryClassName(name: String) = getOption(FACTORY.format(name))
  def getStorageKeySerde(name: String) = getOption(StorageConfig.KEY_SERDE format name)
  def getStorageMsgSerde(name: String) = getOption(StorageConfig.MSG_SERDE format name)
  def getChangelogStream(name: String) = getOption(CHANGELOG_STREAM format name)
  def getStoreNames: Seq[String] = {
    val conf = config.subset("stores.", true)
    conf.keys.filter(k => k.endsWith(".factory")).map(k => k.substring(0, k.length - ".factory".length)).toSeq
  }
}
