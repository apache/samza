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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class TestConfig {
  @Test
  public void testGetShortAndLong() {
    Map<String, String> m = new HashMap<String, String>() {
      {
        put("testkey", "11");
      }
    };

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

  @Test
  public void testSanitize() {
    Map<String, String> m = new HashMap<String, String>() {
      {
        put("key1", "value1");
        put("key2", "value2");
        put("sensitive.key3", "secret1");
        put("sensitive.key4", "secret2");
      }
    };

    Config config = new MapConfig(m);
    assertFalse(config.toString().contains("secret"));

    Config sanitized = config.sanitize();
    assertEquals("value1", sanitized.get("key1"));
    assertEquals("value2", sanitized.get("key2"));
    assertEquals(Config.SENSITIVE_MASK, sanitized.get("sensitive.key3"));
    assertEquals(Config.SENSITIVE_MASK, sanitized.get("sensitive.key4"));
  }

  @Test
  public void testGetList() {
    Map<String, String> m = new HashMap<String, String>() {
      {
        put("key1", " ");
        put("key2", "");
        put("key3", "  value1  ");
        put("key4", "value1,value2");
        put("key5", "value1, value2");
        put("key6", "value1  ,   value2");
      }
    };

    Config config = new MapConfig(m);
    List<String> list = config.getList("key1", Collections.<String>emptyList());
    assertEquals(0, list.size());

    list = config.getList("key2", Collections.<String>emptyList());
    assertEquals(0, list.size());

    list = config.getList("key3");
    assertEquals("  value1  ", list.get(0));

    list = config.getList("key4");
    assertEquals("value1", list.get(0));
    assertEquals("value2", list.get(1));

    list = config.getList("key5");
    assertEquals("value1", list.get(0));
    assertEquals("value2", list.get(1));

    list = config.getList("key6");
    assertEquals("value1", list.get(0));
    assertEquals("value2", list.get(1));

    list = config.getList("UndefinedKey", Collections.<String>emptyList());
    assertEquals(0, list.size());
  }

  @Test
  public void testGetClass() {
    Config config = new MapConfig(ImmutableMap.of("testkey", ArrayList.class.getName()));
    assertEquals(ArrayList.class, config.getClass("testkey"));
  }

  @Test(expected = ConfigException.class)
  public void testGetClassNoEntry() {
    Config config = new MapConfig();
    config.getClass("not a key");
  }

  @Test(expected = ConfigException.class)
  public void testGetClassNoClassFound() {
    Config config = new MapConfig(ImmutableMap.of("testkey", "not a class"));
    config.getClass("testkey");
  }

  @Test
  public void testGetNewInstance() {
    Config config = new MapConfig(ImmutableMap.of("testkey", ArrayList.class.getName()));
    assertTrue(config.getNewInstance("testkey") instanceof ArrayList);
    assertEquals(new ArrayList<>(), config.getNewInstance("testkey"));
  }

  @Test(expected = ConfigException.class)
  public void testGetNewInstanceNoEntry() {
    Config config = new MapConfig();
    config.getNewInstance("not a key");
  }

  @Test(expected = ConfigException.class)
  public void testGetNewInstanceNoClassFound() {
    Config config = new MapConfig(ImmutableMap.of("testkey", "not a class"));
    config.getNewInstance("testkey");
  }

  /**
   * Utility methods to make it easier to tell the class of a primitive via
   * overloaded args
   */
  private Class getClass(long l) {
    return Long.class;
  }

  private Class getClass(short s) {
    return Short.class;
  }

  /**
   * Only loads ArrayList. Throws ClassNotFoundException otherwise.
   */
  private static class ArrayListOnlyClassLoader extends ClassLoader {
    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
      if (ArrayList.class.getName().equals(name)) {
        return ArrayList.class;
      } else {
        throw new ClassNotFoundException();
      }
    }
  }
}
