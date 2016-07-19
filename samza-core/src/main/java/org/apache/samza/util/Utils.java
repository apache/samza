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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction0;


public class Utils {
  private static final Logger log = LoggerFactory.getLogger(Utils.class);

  private Utils() {}

  /**
   * Returns a default value object for scala option.getOrDefault() to use
   * @param value default value
   * @param <T> value type
   * @return object containing default value
   */
  public static <T> AbstractFunction0<T> defaultValue(final T value) {
    return new AbstractFunction0<T>() {
      @Override
      public T apply() {
        return value;
      }
    };
  }

  /**
   * Creates a nanosecond clock using default system nanotime
   * @return object invokes the system clock
   */
  public static AbstractFunction0<Object> defaultClock() {
    return new AbstractFunction0<Object>() {
      @Override
      public Object apply() {
        return System.nanoTime();
      }
    };
  }
}
