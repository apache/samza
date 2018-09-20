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
import org.apache.samza.serializers.ByteSerdeFactory
import org.apache.samza.serializers.DoubleSerdeFactory
import org.apache.samza.serializers.IntegerSerdeFactory
import org.apache.samza.serializers.JsonSerdeFactory
import org.apache.samza.serializers.LongSerdeFactory
import org.apache.samza.config.SerializerConfig.getSerdeFactoryName
import org.apache.samza.serializers.SerializableSerdeFactory
import org.apache.samza.serializers.StringSerdeFactory
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class TestSerializerConfig {
  @Test
  def testGetSerdeFactoryName {
    val config = new MapConfig
    assertEquals(classOf[ByteSerdeFactory].getName, getSerdeFactoryName("byte"))
    assertEquals(classOf[IntegerSerdeFactory].getName, getSerdeFactoryName("integer"))
    assertEquals(classOf[JsonSerdeFactory].getName, getSerdeFactoryName("json"))
    assertEquals(classOf[LongSerdeFactory].getName, getSerdeFactoryName("long"))
    assertEquals(classOf[SerializableSerdeFactory[java.io.Serializable@unchecked]].getName, getSerdeFactoryName("serializable"))
    assertEquals(classOf[StringSerdeFactory].getName, getSerdeFactoryName("string"))
    assertEquals(classOf[DoubleSerdeFactory].getName, getSerdeFactoryName("double"))

    // throw SamzaException if can not find the correct serde
    var throwSamzaException = false
    try {
      getSerdeFactoryName("otherName")
    } catch {
      case e: SamzaException => throwSamzaException = true
      case _: Exception =>
    }
    assertTrue(throwSamzaException)
  }
}
