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

package org.apache.samza.util

import java.util

import joptsimple.util.KeyValuePair
import joptsimple.{ArgumentAcceptingOptionSpec, OptionParser, OptionSet}
import org.apache.samza.config.{Config, ConfigLoaderFactory, MapConfig}

import scala.collection.JavaConverters._

/**
 * Defines a basic set of command-line options for Samza tasks. Tools can use this
 * class directly, or subclass it to add their own options.
 */
class CommandLine {
  val parser = new OptionParser()
  val configOverrideOpt: ArgumentAcceptingOptionSpec[KeyValuePair] =
    parser.accepts("config", "A configuration value in the form key=value. Command line properties override any configuration values given.")
          .withRequiredArg
          .ofType(classOf[KeyValuePair])
          .describedAs("key=value")

  var configLoaderFactory: ConfigLoaderFactory = _

  def loadConfig(options: OptionSet): Config = {
    ConfigUtil.loadConfig(new MapConfig(getConfigOverrides(options)))
  }

  def getConfigOverrides(options: OptionSet): util.Map[String, String] = {
    options.valuesOf(configOverrideOpt).asScala
      .map(kv => (kv.key, kv.value))
      .toMap
      .asJava
  }
}
