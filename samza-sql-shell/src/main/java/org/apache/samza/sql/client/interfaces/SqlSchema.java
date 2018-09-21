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

public class SqlSchema {
    private String[] m_names; // field names
    private String[] m_typeNames; // names of field type

    public SqlSchema(List<String> colNames, List<String> colTypeNames) {
        if(colNames == null || colNames.size() == 0
                ||colTypeNames == null || colTypeNames.size() == 0
                || colNames.size() != colTypeNames.size())
            throw new IllegalArgumentException();

        m_names = new String[colNames.size()];
        m_names = colNames.toArray(m_names);

        m_typeNames = new String[colTypeNames.size()];
        m_typeNames = colTypeNames.toArray(m_typeNames);
    }

    public int getFieldCount() {
        return m_names.length;
    }

    public String getFieldName(int colIdx) {
        return m_names[colIdx];
    }

    public String getFieldTypeName(int colIdx) {
        return m_typeNames[colIdx];
    }
}
