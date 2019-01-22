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

import org.apache.samza.sql.schema.SqlSchema;


/**
 * Execution result of a SELECT statement. It doesn't contain data though.
 */
public class QueryResult {
  private int execId; // execution ID of the statement(s) submitted
  private boolean success; // whether the statement(s) submitted successfully
  private SqlSchema schema; // The schema of the data coming from the query

  public QueryResult(int execId, SqlSchema schema, Boolean success) {
    if (success && schema == null)
      throw new IllegalArgumentException();
    this.execId = execId;
    this.schema = schema;
    this.success = success;
  }

  public int getExecutionId() {
    return execId;
  }

  public SqlSchema getSchema() {
    return schema;
  }

  public boolean succeeded() {
    return success;
  }
}
