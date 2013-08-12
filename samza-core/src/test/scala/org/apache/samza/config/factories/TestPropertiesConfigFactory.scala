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
import java.net.URI
import java.io.File
import org.apache.samza.SamzaException
import org.junit.Assert._
import org.junit.Test

class TestPropertiesConfigFactory {
  val factory = new PropertiesConfigFactory()

  @Test
  def testCanReadPropertiesConfigFiles {
    val config = factory.getConfig(URI.create("file://%s/src/test/resources/test.properties" format new File(".").getCanonicalPath))
    assert("bar".equals(config.get("foo")))
  }

  @Test
  def testCanNotReadNonLocalPropertiesConfigFiles {
    try {
      factory.getConfig(URI.create("hdfs://foo"))
      fail("should have gotten a samza exception")
    } catch {
      case e: SamzaException => None // Do nothing
    }
  }
}
