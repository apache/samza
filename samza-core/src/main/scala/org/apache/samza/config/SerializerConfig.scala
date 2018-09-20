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
import org.apache.samza.serializers.ByteBufferSerdeFactory
import org.apache.samza.serializers.ByteSerdeFactory
import org.apache.samza.serializers.DoubleSerdeFactory
import org.apache.samza.serializers.IntegerSerdeFactory
import org.apache.samza.serializers.JsonSerdeFactory
import org.apache.samza.serializers.LongSerdeFactory
import org.apache.samza.serializers.SerializableSerdeFactory
import org.apache.samza.serializers.StringSerdeFactory
import org.apache.samza.util.Util.info

import scala.collection.JavaConverters._

object SerializerConfig {
  // serializer config constants
  val SERIALIZER_PREFIX = "serializers.registry.%s"
  val SERDE_FACTORY_CLASS = "serializers.registry.%s.class"
  val SERIALIZED_INSTANCE_SUFFIX = ".samza.serialized.instance"
  val SERDE_SERIALIZED_INSTANCE = SERIALIZER_PREFIX + SERIALIZED_INSTANCE_SUFFIX

  implicit def Config2Serializer(config: Config) = new SerializerConfig(config)

  /**
    * Returns the pre-defined serde factory class name for the provided serde name. If no pre-defined factory exists,
    * throws an exception.
    */
  def getSerdeFactoryName(serdeName: String) = {
    val serdeFactoryName = serdeName match {
      case "byte" => classOf[ByteSerdeFactory].getCanonicalName
      case "bytebuffer" => classOf[ByteBufferSerdeFactory].getCanonicalName
      case "integer" => classOf[IntegerSerdeFactory].getCanonicalName
      case "json" => classOf[JsonSerdeFactory].getCanonicalName
      case "long" => classOf[LongSerdeFactory].getCanonicalName
      case "serializable" => classOf[SerializableSerdeFactory[java.io.Serializable]].getCanonicalName
      case "string" => classOf[StringSerdeFactory].getCanonicalName
      case "double" => classOf[DoubleSerdeFactory].getCanonicalName
      case _ => throw new SamzaException("No pre-defined factory class name for serde name %s" format serdeName)
    }
    info("Using default serde %s for serde name %s" format (serdeFactoryName, serdeName))
    serdeFactoryName
  }
}

class SerializerConfig(config: Config) extends ScalaMapConfig(config) {
  def getSerdeClass(name: String) = getOption(SerializerConfig.SERDE_FACTORY_CLASS format name)

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
