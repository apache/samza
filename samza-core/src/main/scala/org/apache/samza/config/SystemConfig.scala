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

import scala.collection.JavaConverters._
import org.apache.samza.util.Logging

/**
  * Note: All new methods are being added to [[org.apache.samza.config.JavaSystemConfig]]
  */
object SystemConfig {
  // system config constants
  val SYSTEM_PREFIX = JavaSystemConfig.SYSTEM_PREFIX + "%s."
  val SYSTEM_FACTORY = JavaSystemConfig.SYSTEM_FACTORY_FORMAT
  val CONSUMER_OFFSET_DEFAULT = SYSTEM_PREFIX + "samza.offset.default"
  val DELETE_MESSAGES_ENABLED = SYSTEM_PREFIX + "samza.delete.messages.enabled"

  implicit def Config2System(config: Config) = new SystemConfig(config)
}

class SystemConfig(config: Config) extends ScalaMapConfig(config) with Logging {
  val javaSystemConfig = new JavaSystemConfig(config)

  def getSystemFactory(name: String) = Option(javaSystemConfig.getSystemFactory(name))

  def getSystemKeySerde(name: String) = getSystemDefaultStreamProperty(name, StreamConfig.KEY_SERDE)

  def getSystemMsgSerde(name: String) = getSystemDefaultStreamProperty(name, StreamConfig.MSG_SERDE)

  def getDefaultSystemOffset(systemName: String) = getOption(SystemConfig.CONSUMER_OFFSET_DEFAULT format (systemName))

  def deleteMessagesEnabled(systemName: String) = getOption(SystemConfig.DELETE_MESSAGES_ENABLED format (systemName))

  /**
   * Returns a list of all system names from the config file. Useful for
   * getting individual systems.
   */
  def getSystemNames() = javaSystemConfig.getSystemNames().asScala

  private def getSystemDefaultStreamProperty(name: String, property: String) = {
    val defaultStreamProperties = javaSystemConfig.getDefaultStreamProperties(name)
    val streamDefault = defaultStreamProperties.get(property)
    if (!(streamDefault == null || streamDefault.isEmpty)) {
      Option(streamDefault)
    } else {
      getNonEmptyOption((SystemConfig.SYSTEM_PREFIX + property) format name)
    }
  }
}
