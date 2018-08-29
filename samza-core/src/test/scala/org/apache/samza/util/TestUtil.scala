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

import org.junit.Assert._
import org.junit.Test
import org.apache.samza.config.MapConfig

class TestUtil {
  @Test
  def testGetLocalHost(): Unit = {
    assertNotNull(Util.getLocalHost)
  }

  @Test
  def testGetObjExistingClass() {
    val obj = Util.getObj("org.apache.samza.config.MapConfig", classOf[MapConfig])
    assertNotNull(obj)
    assertEquals(classOf[MapConfig], obj.getClass())
  }

  @Test(expected = classOf[ClassNotFoundException])
  def testGetObjNonexistentClass() {
    Util.getObj("this.class.does.NotExist", classOf[Object])
    assert(false, "This should not get hit.")
  }
}
