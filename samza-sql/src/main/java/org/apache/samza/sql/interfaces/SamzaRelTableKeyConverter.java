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

import org.apache.samza.sql.SamzaSqlRelRecord;


/**
 * {@link org.apache.samza.sql.translator.SamzaSqlRemoteTableJoinFunction} uses {@link SamzaRelTableKeyConverter}
 * to convert the key to the format expected by the remote table system before doing the table lookup.
 *
 * The {@link SamzaRelTableKeyConverter} is configurable at a system level, so it is possible to configure different
 * {@link SamzaRelTableKeyConverter} for different remote table systems.
 */
public interface SamzaRelTableKeyConverter {
  /**
   * Convert the key in relational record format to the format expected by remote table.
   * @param relKeyRecord key relational record that needs to be converted.
   * @return the table key
   */
  Object convertToTableKeyFormat(SamzaSqlRelRecord relKeyRecord);
}
