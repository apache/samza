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

package org.apache.samza.sql.interfaces;

/**
 * Source Resolvers are used by Samza Sql application to fetch the {@link SqlSystemStreamConfig} corresponding to the source.
 */
public interface SourceResolver {
  /**
   * Returns the SystemStream config corresponding to the source name
   * @param sourceName
   *  source whose systemstreamconfig needs to be fetched.
   * @return
   *  System stream config corresponding to the source.
   */
  SqlSystemStreamConfig fetchSourceInfo(String sourceName);

  /**
   * Returns if a given source is a table. Different source resolvers could have different notations in the source
   * name for denoting a table. Eg: system.stream.$table
   * @param sourceName
   *  source that needs to be checked if it is a table.
   * @return
   *  true if the source is a table, else false.
   */
  boolean isTable(String sourceName);
}