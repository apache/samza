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

package org.apache.samza.sql.schema;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Representation of SQL schema which is used by Samza SQL.
 */
public class SqlSchema {

  public static class SqlField {

    private String fieldName;

    private SqlFieldSchema fieldSchema;

    private int position;

    public SqlField(int pos, String name, SqlFieldSchema schema) {
      position = pos;
      fieldName = name;
      fieldSchema = schema;
    }

    public int getPosition() {
      return position;
    }

    public void setPosition(int position) {
      this.position = position;
    }

    public String getFieldName() {
      return fieldName;
    }

    public void setFieldName(String fieldName) {
      this.fieldName = fieldName;
    }

    public SqlFieldSchema getFieldSchema() {
      return fieldSchema;
    }

    public void setFieldSchema(SqlFieldSchema fieldSchema) {
      this.fieldSchema = fieldSchema;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(SqlSchema.class);

  private List<SqlField> fields;

  public SqlSchema(List<String> colNames, List<SqlFieldSchema> colTypes) {
    if (colNames == null || colTypes == null || colNames.size() != colTypes.size()) {
      throw new IllegalArgumentException();
    }

    fields = IntStream.range(0, colTypes.size())
        .mapToObj(i -> new SqlField(i, colNames.get(i), colTypes.get(i)))
        .collect(Collectors.toList());
  }

  public boolean containsField(String keyName) {
    return fields.stream().anyMatch(x -> x.getFieldName().equals(keyName));
  }

  public List<SqlField> getFields() {
    return fields;
  }
}
