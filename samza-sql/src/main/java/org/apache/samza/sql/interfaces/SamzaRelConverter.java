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

import org.apache.samza.operators.KV;
import org.apache.samza.sql.data.SamzaSqlRelMessage;


/**
 * Samza SQL application uses {@link SamzaRelConverter} to convert the Samza messages to relational message before
 * it can be processed by the calcite engine.
 * The {@link SamzaRelConverter} is configurable at a system level, So it is possible to configure different
 * {@link SamzaRelConverter} for different systems.
 */
public interface SamzaRelConverter {
  /**
   * Converts the object to relational message corresponding to the tableName with relational schema.
   * @param message samza message that needs to be converted.
   * @return Relational message extracted from the object.
   */
  SamzaSqlRelMessage convertToRelMessage(KV<Object, Object> message);

  /**
   * Convert the relational message to the output message.
   * @param relMessage relational message that needs to be converted.
   * @return the key and value of the Samza message
   */
  KV<Object, Object> convertToSamzaMessage(SamzaSqlRelMessage relMessage);
}
