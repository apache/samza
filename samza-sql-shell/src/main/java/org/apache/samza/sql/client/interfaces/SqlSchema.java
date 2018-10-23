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
 * A primitive representation of SQL schema which is just for display purpose.
 */
public class SqlSchema {
  private String[] names; // field names
  private String[] typeNames; // names of field type

  public SqlSchema(List<String> colNames, List<String> colTypeNames) {
    if (colNames == null || colNames.size() == 0
            || colTypeNames == null || colTypeNames.size() == 0
            || colNames.size() != colTypeNames.size())
      throw new IllegalArgumentException();

    names = new String[colNames.size()];
    names = colNames.toArray(names);

    typeNames = new String[colTypeNames.size()];
    typeNames = colTypeNames.toArray(typeNames);
  }

  public int getFieldCount() {
    return names.length;
  }

  public String getFieldName(int colIdx) {
    return names[colIdx];
  }

  public String getFieldTypeName(int colIdx) {
    return typeNames[colIdx];
  }
}
