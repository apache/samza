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

package org.apache.samza.sql.translator;

public class TranslatorConstants {
  public static final String PROCESSING_TIME_NAME = "processingTimeNanos";
  public static final String QUEUEING_LATENCY_MILLIS_NAME = "queueingLatencyMillis";
  public static final String QUERY_LATENCY_NANOS_NAME = "queryLatencyNanos";
  public static final String TOTAL_LATENCY_MILLIS_NAME = "totalLatencyMillis";
  public static final String INPUT_EVENTS_NAME = "inputEvents";
  public static final String FILTERED_EVENTS_NAME = "filteredEvents";
  public static final String OUTPUT_EVENTS_NAME = "outputEvents";
  public static final String LOGOPID_TEMPLATE = "sql_%d_%s_%d";
  public static final String LOGSQLID_TEMPLATE = "sql_%d";
}
