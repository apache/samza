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
import org.apache.samza.sql.schema.SqlSchema;


/**
 * Conventions:
 * <p>
 * Each execution (both query and non-query shall return an non-negative execution ID(execId).
 * Negative execution IDs are reserved for error handling.
 * <p>
 * User shall be able to query the status of an execution even after it's finished, so the
 * executor shall keep record of all the execution unless being asked to remove them (
 * when removeExecution is called.)
 * <p>
 * IMPORTANT: An executor shall support two ways of supplying data:
 * 1. Say user selects profiles of users who visited LinkedIn in the last 5 mins. There could
 * be millions of rows, but the UI only need to show a small fraction. That's retrieveQueryResult,
 * accepting a row range (startRow and endRow). Note that UI may ask for the same data over and over,
 * like when user switches from page 1 to page 2 and data stream changes at the same time, the two
 * pages may actually have overlapped or even same data.
 * <p>
 * 2. Say user wants to see clicks on a LinkedIn page of certain person from now on. In this mode
 * consumeQueryResult shall be used. UI can keep asking for new rows, and once the rows are consumed,
 * it's no longer necessary for the executor to keep them. If lots of rows come in, the UI may be only
 * interested in the last certain rows (as it's in a logview mode), so all data older can be dropped.
 */
public interface SqlExecutor {
  /**
   * SqlExecutor shall be ready to accept all other calls after start() is called.
   * However, it shall NOT store the ExecutionContext for future use, as each
   * call will be given an ExecutionContext which may differ from this one.
   *
   * @param context The ExecutionContext at the time of the call.
   * @throws ExecutorException if the Executor encounters an error.
   */
  void start(ExecutionContext context) throws ExecutorException;

  /**
   * Indicates no further calls will be made thus it's safe for the executor to clean up.
   *
   * @param context The ExecutionContext at the time of the call.
   * @throws ExecutorException if the Executor encounters an error.
   */
  void stop(ExecutionContext context) throws ExecutorException;

  /**
   *
   * @return An EnvironmentVariableHandler that handles executor specific environment variables
   */
  EnvironmentVariableHandler getEnvironmentVariableHandler();

  /**
   * @param context The ExecutionContext at the time of the call.
   * @return A list of table names. Could be empty.
   * @throws ExecutorException if the Executor encounters an error.
   */
  List<String> listTables(ExecutionContext context) throws ExecutorException;

  /**
   * @param context   The ExecutionContext at the time of the call.
   * @param tableName Name of the table to get the schema for.
   * @return Schema of the table.
   * @throws ExecutorException if the Executor encounters an error.
   */
  SqlSchema getTableSchema(ExecutionContext context, String tableName) throws ExecutorException;

  /**
   * @param context   The ExecutionContext at the time of the call.
   * @param statement statement to execute
   * @return The query result.
   * @throws ExecutorException if the Executor encounters an error.
   */
  QueryResult executeQuery(ExecutionContext context, String statement) throws ExecutorException;


  /**
   * @return how many rows available for reading.
   * @throws ExecutorException if the Executor encounters an error.
   */
  int getRowCount() throws ExecutorException;

  /**
   * Row starts at 0. Executor shall keep the data retrieved.
   * For now we get strings for display but we might want strong typed values.
   *
   * @param context  The ExecutionContext at the time of the call.
   * @param startRow Start row index (inclusive)
   * @param endRow   End row index (inclusive)
   * @return A list of row data represented by a String array.
   * @throws ExecutorException if the Executor encounters an error.
   */
  List<String[]> retrieveQueryResult(ExecutionContext context, int startRow, int endRow) throws ExecutorException;


  /**
   * Consumes rows from query result. Executor shall drop them, as "consume" indicates.
   * ALL data before endRow (inclusive, including data before startRow) shall be deleted.
   *
   * @param context  The ExecutionContext at the time of the call.
   * @param startRow Start row index (inclusive)
   * @param endRow   End row index (inclusive)
   * @return available data between startRow and endRow (both are inclusive)
   * @throws ExecutorException if the Executor encounters an error.
   */
  List<String[]> consumeQueryResult(ExecutionContext context, int startRow, int endRow) throws ExecutorException;

  /**
   * Executes all the NON-QUERY statements in the sqlFile.
   * Query statements are ignored as it won't make sense.
   *
   * @param context The ExecutionContext at the time of the call.
   * @param file    A File object to read statements from.
   * @return Execution result.
   * @throws ExecutorException if the Executor encounters an error.
   */
  NonQueryResult executeNonQuery(ExecutionContext context, File file) throws ExecutorException;

  /**
   * @param context    The ExecutionContext at the time of the call.
   * @param statements A list of non-query sql statements.
   * @return Execution result.
   * @throws ExecutorException if the Executor encounters an error.
   */
  NonQueryResult executeNonQuery(ExecutionContext context, List<String> statements) throws ExecutorException;

  /**
   * @param context The ExecutionContext at the time of the call.
   * @param exeId   Execution ID.
   * @throws ExecutorException if the Executor encounters an error.
   */
  void stopExecution(ExecutionContext context, int exeId) throws ExecutorException;


  /**
   * Removing an ongoing execution shall result in an error. Stop it first.
   *
   * @param context The ExecutionContext at the time of the call
   * @param exeId   Execution ID.
   * @throws ExecutorException if the Executor encounters an error.
   */
  void removeExecution(ExecutionContext context, int exeId) throws ExecutorException;

  /**
   * @param execId Execution ID.
   * @return ExecutionStatus.
   * @throws ExecutorException if the Executor encounters an error.
   */
  ExecutionStatus queryExecutionStatus(int execId) throws ExecutorException;

  /**
   * @param context The ExecutionContext at the time of the call.
   * @return A list of SqlFunction.
   * @throws ExecutorException if the Executor encounters an error.
   */
  List<SqlFunction> listFunctions(ExecutionContext context) throws ExecutorException;

  /**
   * Gets the version of this executor.
   * @return A String representing the version of the executor. This function does NOT throw an
   * ExecutorException as the caller has nothing to do to "recover" if the function fails.
   */
  String getVersion();
}
