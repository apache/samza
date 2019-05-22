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
import java.lang.reflect.InvocationTargetException;
import java.util.stream.IntStream;
import org.apache.commons.lang3.Validate;
import org.apache.samza.SamzaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper methods for reflection usage.
 */
public class ReflectionUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ReflectionUtil.class);

  private ReflectionUtil() {
  }

  /**
   * Create an instance of the specified class with the empty constructor.
   *
   * Possible recommendations for the classloader to use:
   * 1) If there is a custom classloader being passed to the caller, consider using that one.
   * 2) If within instance scope, getClass().getClassLoader() can be used in order to use the same classloader as what
   * loaded the caller instance.
   * 3) If within a static/constructor scope, the Class object of the caller (e.g. MyClass.class) can be used.
   *
   * @param <T> type of the object to return
   * @param classLoader used to load the class; if null, will use the bootstrap classloader (see
   * {@link Class#forName(String, boolean, ClassLoader)}
   * @param className name of the class
   * @param clazz type of object to return
   * @return instance of the class
   * @throws SamzaException if an exception was thrown while trying to create the class
   */
  public static <T> T getObj(ClassLoader classLoader, String className, Class<T> clazz) {
    try {
      return doGetObjWithArgs(classLoader, className, clazz);
    } catch (Exception e) {
      String errorMessage = String.format("Unable to create instance for class: %s", className);
      LOG.error(errorMessage, e);
      throw new SamzaException(errorMessage, e);
    }
  }

  /**
   * Create an instance of the specified class with constructor matching the argument array.
   *
   * Possible recommendations for the classloader to use:
   * 1) If there is a custom classloader being passed to the caller, consider using that one.
   * 2) If within instance scope, getClass().getClassLoader() can be used in order to use the same classloader as what
   * loaded the caller instance.
   * 3) If within a static/constructor scope, the Class object of the caller (e.g. MyClass.class) can be used.
   *
   * @param <T> type of the object to return
   * @param classLoader used to load the class; if null, will use the bootstrap classloader (see
   * {@link Class#forName(String, boolean, ClassLoader)}
   * @param className name of the class
   * @param clazz type of object to return
   * @param args arguments to use when calling the constructor for className which corresponds to the types of the args
   * @return instance of the class
   * @throws SamzaException if an exception was thrown while trying to create the class
   */
  public static <T> T getObjWithArgs(ClassLoader classLoader, String className, Class<T> clazz, Object... args) {
    try {
      return doGetObjWithArgs(classLoader, className, clazz, args);
    } catch (Exception e) {
      String errorMessage = String.format("Unable to create instance for class: %s", className);
      LOG.error(errorMessage, e);
      throw new SamzaException(errorMessage, e);
    }
  }

  /**
   * Create an instance of the specified class with constructor matching the argument array.
   *
   * Possible recommendations for the classloader to use:
   * 1) If there is a custom classloader being passed to the caller, consider using that one.
   * 2) If within instance scope, getClass().getClassLoader() can be used in order to use the same classloader as what
   * loaded the caller instance.
   * 3) If within a static/constructor scope, the Class object of the caller (e.g. MyClass.class) can be used.
   *
   * @param <T> type of the object to return
   * @param classLoader used to load the class; if null, will use the bootstrap classloader (see
   * {@link Class#forName(String, boolean, ClassLoader)}
   * @param className name of the class
   * @param clazz type of object to return
   * @param args arguments to use when calling the constructor for className which corresponds to the types of the args
   * @return instance of the class, or null if an exception was thrown while trying to create the instance
   */
  public static <T> T createInstanceOrNull(ClassLoader classLoader, String className, Class<T> clazz, Object... args) {
    try {
      return doGetObjWithArgs(classLoader, className, clazz, args);
    } catch (Exception e) {
      LOG.warn(String.format("Unable to create instance for class: %s", className), e);
      return null;
    }
  }

  /**
   * Create an instance of the specified class with constructor matching the argument array. If there are no args, then
   * this will use the empty constructor.
   *
   * @param <T> type of the object to return
   * @param classLoader used to load the class; if null, will use the bootstrap classloader (see
   * {@link Class#forName(String, boolean, ClassLoader)}
   * @param className name of the class
   * @param clazz type of object to return
   * @param args arguments to use when calling the constructor for className which corresponds to the types of the args
   * @return instance of the class
   */
  private static <T> T doGetObjWithArgs(ClassLoader classLoader, String className, Class<T> clazz, Object... args)
      throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException,
             InstantiationException {
    Validate.notNull(className, "Null class name");

    //noinspection unchecked
    Class<T> classObj = (Class<T>) Class.forName(className, true, classLoader);
    if (args.length == 0) {
      return classObj.newInstance();
    } else {
      Class<?>[] argTypes = new Class<?>[args.length];
      IntStream.range(0, args.length).forEach(i -> argTypes[i] = args[i].getClass());
      Constructor<T> constructor = classObj.getDeclaredConstructor(argTypes);
      return constructor.newInstance(args);
    }
  }
}