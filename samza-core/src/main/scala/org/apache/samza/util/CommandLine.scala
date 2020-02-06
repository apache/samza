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

import joptsimple.{ArgumentAcceptingOptionSpec, OptionParser, OptionSet}
import joptsimple.util.KeyValuePair
import org.apache.samza.config.{Config, ConfigLoaderFactory, JobConfig, MapConfig}
import org.apache.samza.config.loaders.PropertiesConfigLoaderFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Defines a basic set of command-line options for Samza tasks. Tools can use this
 * class directly, or subclass it to add their own options.
 */
class CommandLine {
  val parser = new OptionParser()
  val configLoaderFactoryOpt: ArgumentAcceptingOptionSpec[String] =
    parser.accepts("config-loader-factory", "The config loader factory to use to read full job config file.")
          .withRequiredArg
          .ofType(classOf[java.lang.String])
          .describedAs("com.foo.bar.ClassName")
          .defaultsTo(classOf[PropertiesConfigLoaderFactory].getName)
  val configLoaderPropertiesOpt: ArgumentAcceptingOptionSpec[KeyValuePair] =
    parser.accepts("config-loader-properties", "A config loader property in the form key=value. Config loader properties will be passed to " +
                                               "designated config loader factory to load full job config.")
          .withRequiredArg
          .ofType(classOf[KeyValuePair])
          .describedAs("key=value")
  val configOverrideOpt: ArgumentAcceptingOptionSpec[KeyValuePair] =
    parser.accepts("config", "A configuration value in the form key=value. Command line properties override any configuration values given.")
          .withRequiredArg
          .ofType(classOf[KeyValuePair])
          .describedAs("key=value")

  var configLoaderFactory: ConfigLoaderFactory = _

  def loadConfig(options: OptionSet): Config = {
    // Set up the job parameters.
    val configLoaderFactoryClassName = options.valueOf(configLoaderFactoryOpt)
    val configLoaderProperties = options.valuesOf(configLoaderPropertiesOpt).asScala
      .map(kv => (ConfigLoaderFactory.CONFIG_LOADER_PROPERTIES_PREFIX + kv.key, kv.value))
      .toMap
    val configOverrides = options.valuesOf(configOverrideOpt).asScala
      .map(kv => (kv.key, kv.value))
      .toMap
    val original = mutable.HashMap[String, String]()
    original += JobConfig.CONFIG_LOADER_FACTORY -> configLoaderFactoryClassName
    original ++= configLoaderProperties
    original ++= configOverrides

    ConfigUtil.loadConfig(new MapConfig(original.asJava))
  }
}
