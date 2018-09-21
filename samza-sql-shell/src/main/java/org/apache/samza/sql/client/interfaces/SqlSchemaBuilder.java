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

import java.util.ArrayList;
import java.util.List;

public class SqlSchemaBuilder {
    private List<String> m_names = new ArrayList<>();
    private List<String> m_typeName = new ArrayList<>();

    private SqlSchemaBuilder() {
    }

    public static SqlSchemaBuilder builder() {
        return new SqlSchemaBuilder();
    }

    public SqlSchemaBuilder addField(String name, String fieldType) {
        if(name == null || name.isEmpty() || fieldType == null)
            throw new IllegalArgumentException();

        m_names.add(name);
        m_typeName.add(fieldType);
        return this;
    }

    public SqlSchemaBuilder appendFields(List<String> names, List<String> typeNames) {
        if(names == null || names.size() == 0
                ||typeNames == null || typeNames.size() == 0
                || names.size() != typeNames.size())
            throw new IllegalArgumentException();

        m_names.addAll(names);
        m_typeName.addAll(typeNames);

        return this;
    }

    public SqlSchema toSchema() {
        return new SqlSchema(m_names, m_typeName);
    }
}
