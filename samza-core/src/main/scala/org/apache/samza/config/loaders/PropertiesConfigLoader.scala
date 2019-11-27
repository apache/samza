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

package org.apache.samza.config.loaders

import java.io.FileInputStream
import java.util.Properties

import scala.collection.JavaConverters._
import org.apache.samza.SamzaException
import org.apache.samza.config.{Config, ConfigLoader, MapConfig}
import org.apache.samza.util.Logging

/**
 * Load config from a properties file with given path. "path" will be given in the input properties map.
 */
class PropertiesConfigLoader extends ConfigLoader with Logging {

  def getConfig(properties: java.util.Map[String, String]): Config = {
    val path = properties.get("path")
    if (path == null) {
      throw new SamzaException("path is required to read config from properties file")
    }

    val in = new FileInputStream(path)
    val props = new Properties()

    props.load(in)
    in.close()

    debug("got config %s from path %s" format (props, path))

    new MapConfig(props.asScala.asJava)
  }
}