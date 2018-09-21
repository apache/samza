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


import java.io.File;
import java.util.List;


/**
 *  Conventions:
 *
 *  Implementations shall report UNRECOVERABLE EXCEPTIONS by throwing
 *  ExecutionExceptions, though SqlExecutor doesn't enforce this by as we don't believe in
 *  Java checked exceptions. Report errors by returning values as indicated by each
 *  function and preparing for the subsequent getErrorMsg call.
 *
 *  Each execution (both query and non-query shall return an non-negative execution ID(execId).
 *  Negative execution IDs are reserved for error handling.
 *
 *  User shall be able to query the status of an execution even after it's finished, so the
 *  executor shall keep record of all the execution unless being asked to remove them (
 *  when removeExecution is called.)
 *
 * IMPORTANT: An executor shall support two ways of supplying data:
 * 1. Say user selects profiles of users who visited LinkedIn in the last 5 mins. There could
 *    be millions of rows, but the UI only need to show a small fraction. That's retrieveQueryResult,
 *    accepting a row range (startRow and endRow). Note that UI may ask for the same data over and over,
 *    like when user switches from page 1 to page 2 and data stream changes at the same time, the two
 *    pages may actually have overlapped or even same data.
 *
 * 2. Say user wants to see clicks on a LinkedIn page of certain person from now on. In this mode
 *    consumeQueryResult shall be used. UI can keep asking for new rows, and once the rows are consumed,
 *    it's no longer necessary for the executor to keep them. If lots of rows come in, the UI may be only
 *    interested in the last certain rows (as it's in a logview mode), so all data older can be dropped.
 *
 */
public interface SqlExecutor {
    /**
     *  SqlExecutor shall be ready to accept all other calls after start() is called.
     *  However, it shall NOT store the ExecutionContext for future use, as each
     *  call will be given an ExecutionContext which may differ from this one.
     */
    public void start(ExecutionContext context);

    /**
     * Indicates no further calls will be made thus it's safe for the executor to clean up.
     */
    public void stop(ExecutionContext context);

    /**
     * @return null if an error occurs. Prepare for subsequent getErrorMsg call.
     *         an empty list indicates no tables found.
     */
    public List<String> listTables(ExecutionContext context);

    /**
     * @return null if an error occurs. Prepare for subsequent getErrorMsg call.
     */
    public SqlSchema getTableScema(ExecutionContext context, String tableName);

    /**
     */
    public QueryResult executeQuery(ExecutionContext context, String statement);



    /**
     * @return how many rows for reading.
     */
    public int getRowCount();

    /**
     * Row starts at 0. Executor shall keep the data retrieved.
     * For now we get strings for display but we might want strong typed values.
     */
    public List<String[]> retrieveQueryResult(ExecutionContext context, int startRow, int endRow);


    /**
     * Consumes rows from query result. Executor shall drop them, as "consume" indicates.
     * All the data before endRow (inclusive) will be deleted.
     * @return available data between startRow and endRow (both are inclusive)
     */
    // For logging view mode. Still not sure what the interface should be like.
    // Don't support this method for now.
    public List<String[]> consumeQueryResult(ExecutionContext context, int startRow, int endRow);

    /**
     * Executes all the NON-QUERY statements in the sqlFile.
     * Query statements are ignored as it won't make sense.
     */
    public NonQueryResult executeNonQuery(ExecutionContext context, File file);

    /**
     */
    public NonQueryResult executeNonQuery(ExecutionContext context, List<String> statements);

    /**
     */
    public boolean stopExecution(ExecutionContext context, int exeId);


    /**
     *  Removing an ongoing execution shall result in an error. Stop it first.
     */
    public boolean removeExecution(ExecutionContext context, int exeId);

    /**
     *
     */
    public ExecutionStatus queryExecutionStatus(int execId);

    /**
     *
     */
    public String getErrorMsg();

    /**
     *
     * @param m_exeContext
     * @return
     */
    List<SqlFunction> listFunctions(ExecutionContext m_exeContext);
}
