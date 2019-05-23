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

import java.lang.reflect.Constructor;
import java.util.stream.IntStream;
import org.apache.commons.lang3.Validate;
import org.apache.samza.SamzaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ReflectionUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ReflectionUtil.class);

  private ReflectionUtil() {
  }

  /**
   * Instantiate an object of type T from a given className using the given classLoader.
   *
   * @param className full class name (including namespace)
   * @param clazz type of object to return
   * @param classLoader classloader to use to load className
   * @return object of type T created from the empty constructor of the class corresponding to className
   * @throws SamzaException if class instance cannot be created
   */
  public static <T> T getObj(String className, Class<T> clazz, ClassLoader classLoader) {
    try {
      //noinspection unchecked
      return (T) Class.forName(className, true, classLoader).newInstance();
    } catch (Exception e) {
      String errorMessage = String.format("Unable to create instance for class: %s", className);
      LOG.error(errorMessage, e);
      throw new SamzaException(errorMessage, e);
    }
  }

  /**
   * Create an instance of the specified class with constructor matching the argument array.
   * @param className name of the class
   * @param clazz type of object to return
   * @param classLoader {@link ClassLoader} to use for loading the class
   * @param args argument array
   * @param <T> type fo the class
   * @return instance of the class
   */
  public static <T> T getObjWithArgs(String className, Class<T> clazz, ClassLoader classLoader, Object... args) {
    Validate.notNull(className, "null class name");
    try {
      //noinspection unchecked
      Class<T> classObj = (Class<T>) Class.forName(className, true, classLoader);
      Class<?>[] argTypes = new Class<?>[args.length];
      IntStream.range(0, args.length).forEach(i -> argTypes[i] = args[i].getClass());
      Constructor<T> ctor = classObj.getDeclaredConstructor(argTypes);
      return ctor.newInstance(args);
    } catch (Exception e) {
      LOG.warn("Failed to create instance for: " + className, e);
      throw new SamzaException(e);
    }
  }

  /**
   * Create an instance of the specified class with constructor matching the argument array.
   * @param className name of the class
   * @param clazz type of object to return
   * @param classLoader {@link ClassLoader} to use for loading the class
   * @param args argument array
   * @param <T> type fo the class
   * @return instance of the class, or null if anything went wrong
   */
  public static <T> T createInstanceOrNull(String className, Class<T> clazz, ClassLoader classLoader, Object... args) {
    try {
      return getObjWithArgs(className, clazz, classLoader, args);
    } catch (Exception e) {
      // exception logging gets done in getObjWithArgs
      return null;
    }
  }
}