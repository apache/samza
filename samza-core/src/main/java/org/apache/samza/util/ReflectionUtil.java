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
   * @param <T> type of the object to return
   * @param className name of the class
   * @param clazz type of object to return
   * @return instance of the class
   * @throws SamzaException if an exception was thrown while trying to create the class
   */
  public static <T> T getObj(String className, Class<T> clazz) {
    return doGetObjWithArgs(className, clazz);
  }

  /**
   * Create an instance of the specified class with constructor matching the arguments. Make sure that the types passed
   * using {@link ConstructorArgument} match the compile-time types of the arguments of the constructor that should be
   * used.
   *
   * @param <T> type of the object to return
   * @param className name of the class
   * @param clazz type of object to return
   * @param args arguments with types for finding and calling desired constructor to create an instance of className
   * @return instance of the class
   * @throws SamzaException if an exception was thrown while trying to create the class
   */
  public static <T> T getObjWithArgs(String className, Class<T> clazz, ConstructorArgument<?>... args) {
    return doGetObjWithArgs(className, clazz, args);
  }

  /**
   * Use this to create arguments for when calling
   * {@link #doGetObjWithArgs(String, Class, ConstructorArgument[])}
   */
  public static <T> ConstructorArgument<T> constructorArgument(T value, Class<T> clazz) {
    return new ConstructorArgument<>(value, clazz);
  }

  /**
   * Create an instance of the specified class with constructor matching the argument array. If there are no args, then
   * this will use the empty constructor. Make sure that the types passed using {@link ConstructorArgument} match the
   * compile-time types of the arguments of the constructor that should be used.
   *
   * @param <T> type of the object to return
   * @param className name of the class
   * @param clazz type of object to return
   * @param args arguments with types for finding and calling desired constructor to create an instance of className
   * @return instance of the class
   * @throws SamzaException if an exception was thrown while trying to create the class
   */
  private static <T> T doGetObjWithArgs(String className, Class<T> clazz, ConstructorArgument<?>... args) {
    Validate.notNull(className, "Null class name");

    try {
      //noinspection unchecked
      Class<T> classObj = (Class<T>) Class.forName(className);
      if (args.length == 0) {
        return classObj.newInstance();
      } else {
        // can't just use getClass on argument values since runtime types might not exactly match constructor signatures
        Class<?>[] argClasses = new Class<?>[args.length];
        Object[] argValues = new Object[args.length];
        IntStream.range(0, args.length).forEach(i -> {
            ConstructorArgument<?> constructorArgument = args[i];
            argClasses[i] = constructorArgument.getClazz();
            argValues[i] = constructorArgument.getValue();
          });
        Constructor<T> constructor = classObj.getDeclaredConstructor(argClasses);
        return constructor.newInstance(argValues);
      }
    } catch (Exception e) {
      String errorMessage = String.format("Unable to create instance for class: %s", className);
      LOG.error(errorMessage, e);
      throw new SamzaException(errorMessage, e);
    }
  }

  /**
   * This is public because the compiler complains when using a non-visible modifier with varargs.
   * Use {@link #constructorArgument(Object, Class)} to build an instance of this.
   */
  public static class ConstructorArgument<T> {
    private final T value;
    private final Class<T> clazz;

    private ConstructorArgument(T value, Class<T> clazz) {
      this.value = value;
      this.clazz = clazz;
    }

    private T getValue() {
      return value;
    }

    private Class<T> getClazz() {
      return clazz;
    }
  }
}