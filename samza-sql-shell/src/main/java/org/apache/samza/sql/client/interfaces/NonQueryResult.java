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
 * Result of a non-query SQL statement or SQL file containing multiple non-query statements.
 */
public class NonQueryResult {
  private int execId; // execution ID of the statement(s) submitted

  // When user submits a batch of SQL statements, only the non-query ones will be submitted
  private List<String> submittedStmts;
  private List<String> nonSubmittedStmts;

  public NonQueryResult(int execId) {
    this.execId = execId;
  }

  public NonQueryResult(int execId, List<String> submittedStmts, List<String> nonSubmittedStmts) {
    this.execId = execId;
    this.submittedStmts = submittedStmts;
    this.nonSubmittedStmts = nonSubmittedStmts;
  }

  public int getExecutionId() {
    return execId;
  }

  public List<String> getSubmittedStmts() {
    return submittedStmts;
  }

  public List<String> getNonSubmittedStmts() {
    return nonSubmittedStmts;
  }
}
