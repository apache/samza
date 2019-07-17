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
package org.apache.samza.testUtils;

import com.google.common.base.Preconditions;
import java.lang.reflect.Field;
import org.apache.samza.SamzaException;


/**
 * A collection of utility methods used for testing
 */
public class TestUtils {
  /**
   * Get the value of the specified field from the target object
   * @param target target object
   * @param fieldName name of the field
   * @param <T> type of retrieved value
   * @return value of the field
   */
  public static <T> T getFieldValue(Object target, String fieldName) {

    Preconditions.checkNotNull(target, "Target object is null");
    Preconditions.checkNotNull(fieldName, "Field name is null");

    Field field = null;
    Boolean prevAccessible = null;
    try {
      field = target.getClass().getDeclaredField(fieldName);
      prevAccessible = field.isAccessible();
      field.setAccessible(true);
      return (T) field.get(target);
    } catch (Exception ex) {
      throw new SamzaException(ex);
    } finally {
      if (field != null && prevAccessible != null) {
        field.setAccessible(prevAccessible);
      }
    }
  }
}
