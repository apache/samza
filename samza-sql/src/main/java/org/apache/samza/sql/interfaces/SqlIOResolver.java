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
 * IO Resolvers are used by Samza SQL application to fetch the {@link SqlIOConfig} corresponding
 * to the input and output system, including both Samza stream and table systems.
 */
public interface SqlIOResolver {
  /**
   * Returns the SQL IO config corresponding to the source name
   * @param sourceName
   *  source whose IOConfig needs to be fetched.
   * @return
   *  IOConfig corresponding to the source.
   */
  SqlIOConfig fetchSourceInfo(String sourceName);

  /**
   * Returns the SQL IO config corresponding to the sink name
   * @param sinkName
   *  sink whose IOConfig needs to be fetched.
   * @return
   *  IOConfig corresponding to the sink.
   */
  SqlIOConfig fetchSinkInfo(String sinkName);
}