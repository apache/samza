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

package org.apache.samza.sql.client.interfaces;

import java.util.List;

/**
 * Represents a SQL function.
 */
public interface SqlFunction {
  /**
   * Gets the name of the function.
   * @return name of the function
   */
  public String getName();

  /**
   * Gets the description of the function.
   * @return description of the function.
   */
  public String getDescription();

  /**
   * Gets the argument types of the function as a List.
   * @return A list containing the type names of the arguments.
   */
  public List<String> getArgumentTypes();

  /**
   * Gets the return type of the function.
   * @return return type name
   */
  public String getReturnType();

  /**
   * Don't forget to implement toString()
   */
}
