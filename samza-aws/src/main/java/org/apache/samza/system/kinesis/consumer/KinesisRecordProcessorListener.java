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

package org.apache.samza.system.kinesis.consumer;

import java.util.List;

import org.apache.samza.system.SystemStreamPartition;

import com.amazonaws.services.kinesis.model.Record;


/**
 * Listener interface implemented by consumer to be notified when {@link KinesisRecordProcessor} receives records and
 * is ready to shutdown.
 */
public interface KinesisRecordProcessorListener {
  /**
   * Method invoked by
   * {@link KinesisRecordProcessor#processRecords(com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput)}
   * when the records are received by the processor.
   * @param ssp Samza partition for which the records belong to
   * @param records List of kinesis records
   * @param millisBehindLatest Time lag of the batch of records with respect to the tip of the stream
   */
  void onReceiveRecords(SystemStreamPartition ssp, List<Record> records, long millisBehindLatest);

  /**
   * Method invoked by
   * {@link KinesisRecordProcessor#shutdown(com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput)}
   * when the processor is ready to shutdown.
   * @param ssp Samza partition for which the shutdown is invoked
   */
  void onShutdown(SystemStreamPartition ssp);
}
