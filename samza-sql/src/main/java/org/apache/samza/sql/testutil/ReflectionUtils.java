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

package org.apache.samza.sql.testutil;

import java.lang.reflect.Constructor;
import java.util.stream.IntStream;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class to simplify usage of Java reflection.
 */
public class ReflectionUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ReflectionUtils.class);

  private ReflectionUtils() {

  }

  /**
   * Create an instance of the specified class with constuctor
   * matching the argument array.
   * @param clazz name of the class
   * @param args argument array
   * @param <T> type fo the class
   * @return instance of the class, or null if anything went wrong
   */
  @SuppressWarnings("unchecked")
  public static <T> T createInstance(String clazz, Object... args) {
    Validate.notNull(clazz, "null class name");
    try {
      Class<T> classObj = (Class<T>) Class.forName(clazz);
      Class<?>[] argTypes = new Class<?>[args.length];
      IntStream.range(0, args.length).forEach(i -> argTypes[i] = args[i].getClass());
      Constructor<T> ctor = classObj.getDeclaredConstructor(argTypes);
      return ctor.newInstance(args);
    } catch (Exception e) {
      LOG.warn("Failed to create instance for: " + clazz, e);
      return null;
    }
  }
}
