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

object SerializerConfig {
  // serializer config constants
  val SERIALIZER_PREFIX = "serializers.registry.%s"
  val SERDE = "serializers.registry.%s.class"

  implicit def Config2Serializer(config: Config) = new SerializerConfig(config)
}

class SerializerConfig(config: Config) extends ScalaMapConfig(config) {
  def getSerdeClass(name: String) = getOption(SerializerConfig.SERDE format name)

  /**
   * Returns a list of all serializer names from the config file. Useful for
   * getting individual serializers.
   */
  import SerializerConfig._
  def getSerdeNames() = {
    val subConf = config.subset(SERIALIZER_PREFIX format "", true)
    subConf.asScala.keys.filter(k => k.endsWith(".class")).map(_.replace(".class", ""))
  }
}
