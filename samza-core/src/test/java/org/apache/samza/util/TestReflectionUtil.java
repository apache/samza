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
package org.apache.samza.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.samza.SamzaException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class TestReflectionUtil {
  @Test
  public void testGetObj() {
    // using caller classloader
    assertTrue(
        ReflectionUtil.getObj(ArrayList.class.getName(), List.class, getClass().getClassLoader()) instanceof ArrayList);
    assertEquals(new ArrayList<>(),
        ReflectionUtil.getObj(ArrayList.class.getName(), List.class, getClass().getClassLoader()));

    // using custom classloader
    assertTrue(ReflectionUtil.getObj(ArrayList.class.getName(), List.class,
        new ArrayListOnlyClassLoader()) instanceof ArrayList);
    assertEquals(new ArrayList<>(),
        ReflectionUtil.getObj(ArrayList.class.getName(), List.class, new ArrayListOnlyClassLoader()));
  }

  /**
   * This test verifies two things:
   * 1) Exception is handled in the expected way
   * 2) The classloader passed as an argument gets used for classloading
   */
  @Test(expected = SamzaException.class)
  public void testGetObjNoClass() {
    ReflectionUtil.getObj(HashSet.class.getName(), Set.class, new ArrayListOnlyClassLoader());
  }

  @Test(expected = SamzaException.class)
  public void testGetObjInvalidConstructor() {
    ReflectionUtil.getObj(WithTwoArgConstructor.class.getName(), Object.class, getClass().getClassLoader());
  }

  @Test
  public void testGetObjWithArgs() {
    assertEquals(new WithTwoArgConstructor("hello", "world"),
        ReflectionUtil.getObjWithArgs(WithTwoArgConstructor.class.getName(), WithTwoArgConstructor.class,
            getClass().getClassLoader(), "hello", "world"));
  }

  /**
   * This test verifies two things:
   * 1) Exception is handled in the expected way
   * 2) The classloader passed as an argument gets used for classloading
   */
  @Test(expected = SamzaException.class)
  public void testGetObjWithArgsNoClass() {
    ReflectionUtil.getObjWithArgs(HashSet.class.getName(), Set.class, new ArrayListOnlyClassLoader(), 10);
  }

  @Test(expected = SamzaException.class)
  public void testGetObjWithArgsInvalidConstructor() {
    ReflectionUtil.getObjWithArgs(WithTwoArgConstructor.class.getName(), Object.class, getClass().getClassLoader(),
        "hello world");
  }

  @Test
  public void testCreateInstanceOrNull() {
    assertEquals(new WithTwoArgConstructor("hello", "world"),
        ReflectionUtil.createInstanceOrNull(WithTwoArgConstructor.class.getName(), WithTwoArgConstructor.class,
            getClass().getClassLoader(), "hello", "world"));
  }

  @Test
  public void testCreateInstanceOrNullInvalid() {
    // no class exists (also verifies classloader passed as argument gets used)
    assertNull(
        ReflectionUtil.createInstanceOrNull(HashSet.class.getName(), Set.class, new ArrayListOnlyClassLoader(), 10));

    // class doesn't have a matching constructor
    assertNull(ReflectionUtil.createInstanceOrNull(WithTwoArgConstructor.class.getName(), Object.class,
        getClass().getClassLoader(), "hello world"));
  }

  private static class WithTwoArgConstructor {
    private final String s0;
    private final String s1;

    WithTwoArgConstructor(String s0, String s1) {
      // just need some constructor so that there is no default constructor
      this.s0 = s0;
      this.s1 = s1;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      WithTwoArgConstructor that = (WithTwoArgConstructor) o;
      return Objects.equals(s0, that.s0) && Objects.equals(s1, that.s1);
    }

    @Override
    public int hashCode() {
      return Objects.hash(s0, s1);
    }
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