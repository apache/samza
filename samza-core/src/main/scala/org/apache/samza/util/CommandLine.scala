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

import java.net.URI
import joptsimple.{OptionParser, OptionSet}
import joptsimple.util.KeyValuePair
import org.apache.samza.config.{ConfigFactory, MapConfig}
import org.apache.samza.config.factories.PropertiesConfigFactory
import scala.collection.mutable.Buffer
import scala.collection.JavaConversions._

/**
 * Defines a basic set of command-line options for Samza tasks. Tools can use this
 * class directly, or subclass it to add their own options.
 */
class CommandLine {
  val parser = new OptionParser()
  val configFactoryOpt = 
    parser.accepts("config-factory", "The config factory to use to read your config file.")
          .withRequiredArg
          .ofType(classOf[java.lang.String])
          .describedAs("com.foo.bar.ClassName")
          .defaultsTo(classOf[PropertiesConfigFactory].getName)
  val configPathOpt =
    parser.accepts("config-path", "URI location to a config file (e.g. file:///some/local/path.properties). " + 
                                  "If multiple files are given they are all used with later files overriding any values that appear in earlier files.")
          .withRequiredArg
          .ofType(classOf[URI])
          .describedAs("path")
  val configOverrideOpt = 
    parser.accepts("config", "A configuration value in the form key=value. Command line properties override any configuration values given.")
          .withRequiredArg
          .ofType(classOf[KeyValuePair])
          .describedAs("key=value")

  var configFactory: ConfigFactory = null

  def loadConfig(options: OptionSet) = {
    // Verify legitimate parameters.
    if (!options.has(configPathOpt)) {
      parser.printHelpOn(System.err)
      System.exit(-1)
    }

    // Set up the job parameters.
    val configFactoryClass = options.valueOf(configFactoryOpt)
    val configPaths = options.valuesOf(configPathOpt)
    configFactory = ClassLoaderHelper.fromClassName[ConfigFactory](configFactoryClass)
    val configOverrides = options.valuesOf(configOverrideOpt).map(kv => (kv.key, kv.value)).toMap

    val configs: Buffer[java.util.Map[String, String]] = configPaths.map(configFactory.getConfig)
    configs += configOverrides
    new MapConfig(configs)
  }
}
