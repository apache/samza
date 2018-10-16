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

public class NonQueryResult {
    private int m_execId;
    private boolean m_success;
    private List<String> m_submittedStmts;
    private List<String> m_nonSubmittedStmts;

    public NonQueryResult(int execId, boolean success) {
        m_execId = execId;
        m_success = success;
    }

    public NonQueryResult(int execId, boolean success, List<String> submittedStmts, List<String> nonSubmittedStmts) {
        m_execId = execId;
        m_success = success;
        m_submittedStmts = submittedStmts;
        m_nonSubmittedStmts = nonSubmittedStmts;
    }

    public int getExecutionId() {
        return m_execId;
    }

    public boolean succeeded() {
        return m_success;
    }

    public List<String> getSubmittedStmts() {
        return m_submittedStmts;
    }

    public List<String> getNonSubmittedStmts() {
        return m_nonSubmittedStmts;
    }
}
