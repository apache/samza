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

package org.apache.samza.config.factories
import java.io.FileInputStream
import java.net.URI
import java.util.Properties
import scala.collection.JavaConversions._
import org.apache.samza.config.Config
import org.apache.samza.config.ConfigFactory
import org.apache.samza.config.MapConfig
import org.apache.samza.util.Logging
import org.apache.samza.SamzaException

class PropertiesConfigFactory extends ConfigFactory with Logging {
  def getConfig(configUri: URI): Config = {
    val scheme = configUri.getScheme
    if (scheme != null && !scheme.equals("file")) {
      throw new SamzaException("only the file:// scheme is supported for properties files")
    }

    val configPath = configUri.getPath
    val props = new Properties();
    val in = new FileInputStream(configPath);

    props.load(in);
    in.close

    debug("got config %s from config %s" format (props, configPath))

    new MapConfig(props.toMap[String, String])
  }
}
