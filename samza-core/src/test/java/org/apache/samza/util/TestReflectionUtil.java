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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.samza.SamzaException;
import org.junit.Test;

import static org.junit.Assert.*;


public class TestReflectionUtil {
  @Test
  public void testGetObj() {
    assertTrue(ReflectionUtil.getObj(ArrayList.class.getName(), List.class) instanceof ArrayList);
    assertEquals(new ArrayList<>(), ReflectionUtil.getObj(ArrayList.class.getName(), List.class));
  }

  @Test(expected = SamzaException.class)
  public void testGetObjNoClass() {
    ReflectionUtil.getObj("not.a.class", Set.class);
  }

  @Test(expected = SamzaException.class)
  public void testGetObjInvalidConstructor() {
    ReflectionUtil.getObj(WithTwoArgConstructor.class.getName(), Object.class);
  }

  @Test
  public void testGetObjWithArgs() {
    assertEquals(new WithTwoArgConstructor("hello", ImmutableList.of("hello", "world")),
        ReflectionUtil.getObjWithArgs(WithTwoArgConstructor.class.getName(),
            WithTwoArgConstructor.class,
            ReflectionUtil.constructorArgument("hello", String.class),
            ReflectionUtil.constructorArgument(ImmutableList.of("hello", "world"), List.class)));

    // should still work if pass no args, since should use empty constructor
    assertTrue(ReflectionUtil.getObjWithArgs(ArrayList.class.getName(), List.class) instanceof ArrayList);
    assertEquals(new ArrayList<>(), ReflectionUtil.getObjWithArgs(ArrayList.class.getName(), List.class));
  }

  @Test(expected = SamzaException.class)
  public void testGetObjWithArgsNoClass() {
    ReflectionUtil.getObjWithArgs("not.a.class", Set.class, ReflectionUtil.constructorArgument(10, Integer.class));
  }

  @Test(expected = SamzaException.class)
  public void testGetObjWithArgsWrongArgumentCount() {
    ReflectionUtil.getObjWithArgs(WithTwoArgConstructor.class.getName(), Object.class,
        ReflectionUtil.constructorArgument("hello world", String.class));
  }

  @Test(expected = SamzaException.class)
  public void testGetObjWithArgsWrongArgumentTypes() {
    ReflectionUtil.getObjWithArgs(WithTwoArgConstructor.class.getName(),
        Object.class,
        ReflectionUtil.constructorArgument("hello world", String.class),
        ReflectionUtil.constructorArgument(ImmutableList.of("hello", "world"), ImmutableList.class));
  }

  @Test(expected = SamzaException.class)
  public void testGetObjWithArgsZeroArgsNoClass() {
    ReflectionUtil.getObjWithArgs("not.a.class", Set.class);
  }

  @Test(expected = SamzaException.class)
  public void testGetObjWithArgsZeroArgsInvalidConstructor() {
    ReflectionUtil.getObjWithArgs(WithTwoArgConstructor.class.getName(), Object.class);
  }

  private static class WithTwoArgConstructor {
    private final String string;
    private final List<String> list;

    WithTwoArgConstructor(String string, List<String> list) {
      // just need some constructor so that there is no default constructor
      this.string = string;
      this.list = list;
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
      return Objects.equals(string, that.string) && Objects.equals(list, that.list);
    }

    @Override
    public int hashCode() {
      return Objects.hash(string, list);
    }
  }
}