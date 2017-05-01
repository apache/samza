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
  val SYSTEM_PREFIX = "systems.%s."
  val SYSTEM_FACTORY = "systems.%s.samza.factory"
  val CONSUMER_OFFSET_DEFAULT = SYSTEM_PREFIX + "samza.offset.default"

  implicit def Config2System(config: Config) = new SystemConfig(config)
}

class SystemConfig(config: Config) extends ScalaMapConfig(config) with Logging {
  def getSystemFactory(name: String) = getOption(SystemConfig.SYSTEM_FACTORY format name)

  def getSystemKeySerde(name: String) = getSystemDefaultStreamProperty(name, StreamConfig.KEY_SERDE)

  def getSystemMsgSerde(name: String) = getSystemDefaultStreamProperty(name, StreamConfig.MSG_SERDE)

  def getDefaultSystemOffset(systemName: String) = getOption(SystemConfig.CONSUMER_OFFSET_DEFAULT format (systemName))

  /**
   * Returns a list of all system names from the config file. Useful for
   * getting individual systems.
   */
  def getSystemNames() = {
    val subConf = config.subset("systems.", true)
    // find all .samza.factory keys, and strip the suffix
    subConf.asScala.keys.filter(k => k.endsWith(".samza.factory")).map(_.replace(".samza.factory", ""))
  }

  private def getSystemDefaultStreamProperty(name: String, property: String) = {
    val defaultStreamProperties = new JavaSystemConfig(config).getDefaultStreamProperties(name)
    val streamDefault = defaultStreamProperties.get(property)
    if (!(streamDefault == null || streamDefault.isEmpty)) {
      Option(streamDefault)
    } else {
      getNonEmptyOption((SystemConfig.SYSTEM_PREFIX + property) format name)
    }
  }
}
