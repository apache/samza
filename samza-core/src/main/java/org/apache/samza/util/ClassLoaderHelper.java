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

import org.apache.samza.config.ConfigException;

import java.lang.reflect.Constructor;

public class ClassLoaderHelper {

  public static <T> T fromClassName(String className) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    Class<T> clazz = (Class<T>) Class.forName(className);
    T instance = clazz.newInstance();
    return instance;
  }

  public static <T> T fromClassName(String className, Class<T> classType) {
    try {
      Class<?> idGeneratorClass = Class.forName(className);
      if (!classType.isAssignableFrom(idGeneratorClass)) {
        throw new ConfigException(String.format(
            "Class %s is not of type %s", className, classType));
      }
      Constructor<?> constructor = idGeneratorClass.getConstructor();
      return (T) constructor.newInstance();
    } catch (Exception e) {
      throw new ConfigException(String.format(
          "Problem in loading %s class %s", classType, className), e);
    }
  }
}