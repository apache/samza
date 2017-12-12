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

package org.apache.samza.sql.udfs;

import org.apache.samza.config.Config;


/**
 * The base class for the Scalar UDFs. All the scalar UDF classes needs to extend this and implement a method named
 * "execute". The number and type of arguments for the execute method in the UDF class should match the number and type of fields
 * used while invoking this UDF in SQL statement.
 * Say for e.g. User creates a UDF class with signature int execute(int var1, String var2). It can be used in a SQL query
 *     select myudf(id, name) from profile
 * In the above query, Profile should contain fields named 'id' of INTEGER/NUMBER type and 'name' of type VARCHAR/CHARACTER
 */
public interface ScalarUdf<T> {
  /**
   * Udfs can implement this method to perform any initialization that they may need.
   * @param udfConfig Config specific to the udf.
   */
  void init(Config udfConfig);

  /**
   * Actual implementation of the udf function
   * @param args
   *   list of all arguments that the udf needs
   * @return
   *   Return value from the scalar udf.
   */
  T execute(Object... args);
}
