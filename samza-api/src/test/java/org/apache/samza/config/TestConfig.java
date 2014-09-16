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

package org.apache.samza.config;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.HashMap;

import org.junit.Test;

public class TestConfig {
  /**
   * Utility methods to make it easier to tell the class of a primitive via
   * overloaded args
   */
  Class getClass(long l) {
    return Long.class ;
  }

  Class getClass(short s) {
    return Short.class ;
  }

  @Test
  public void testgetShortAndLong() {
    Map<String, String> m = new HashMap<String, String>() {{
      put("testkey", "11");
    }};

    MapConfig mc = new MapConfig(m);
    short defaultShort = 0;
    long defaultLong = 0;

    Class c1 = getClass(mc.getShort("testkey"));
    assertEquals(Short.class, c1);

    Class c2 = getClass(mc.getShort("testkey", defaultShort));
    assertEquals(Short.class, c2);

    Class c3 = getClass(mc.getLong("testkey"));
    assertEquals(Long.class, c3);

    Class c4 = getClass(mc.getLong("testkey", defaultLong));
    assertEquals(Long.class, c4);
  }
}
