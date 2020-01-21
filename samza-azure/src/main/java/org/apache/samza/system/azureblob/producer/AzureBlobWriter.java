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

package org.apache.samza.system.azureblob.producer;

import java.io.IOException;
import org.apache.samza.system.OutgoingMessageEnvelope;

/**
 * {@link org.apache.samza.system.azureblob.producer.AzureBlobSystemProducer}
 *  uses an AzureBlobWriter to write messages to Azure Blob Storage.
 *
 *  Implementation is expected to be thread-safe.
 */
public interface AzureBlobWriter {
  /**
   * Write the given {@link org.apache.samza.system.OutgoingMessageEnvelope} to the blob opened.
   * @param ome message to be written
   */
  void write(OutgoingMessageEnvelope ome) throws IOException;

  /**
   * Asynchronously upload the messages written as a block.
   * After this the messages written will go as a new block.
   */
  void flush() throws IOException;

  /**
   * Close the writer and all of its underlying components.
   * At the end of close, all the messages sent to the writer should be persisted in a blob.
   * flush should be called explicitly before close.
   * It is not the responsibility of close to upload blocks.
   * After close, no other operations can be performed.
   */
  void close() throws IOException;
}
