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

package org.apache.samza.system.chooser

import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.metrics.MetricsHelper
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.util.Logging

/**
 * BatchingChooser provides a batching functionality on top of an existing
 * MessageChooser. This is useful in cases where batching leads to better
 * performance.
 *
 * BatchingChooser wraps an underlying MessageChooser. Initially,
 * BatchingChooser simply forwards update and choose calls to the wrapped
 * MessageChooser. The first time that the wrapped MessageChooser returns a
 * non-null envelope, BatchingChooser holds on to the SystemStreamPartition
 * from the envelope.
 *
 * As future envelopes arrive via BatchingChooser.update, BatchingChooser
 * looks at the SSP for each envelope. If the SSP is the same as the SSP for
 * the last envelope that was returned by the wrapped MessageChooser's choose
 * method, BatchingChooser caches the new envelope rather than forwarding it to
 * the wrapped chooser. The next time choose is called, BatchingChooser will
 * return the cached envelope, if it is non-null, instead of calling choose on
 * the wrapped MessageChooser.
 *
 * BatchingChooser keeps doing this until the batch size limit has been reached,
 * or BatchingChooser.choose is called, and no envelope is available for the
 * batched SSP. In either of these cases, the batch is reset, and a new SSP is
 * chosen.
 *
 * This class depends on the contract defined in MessageChooser. Specifically,
 * it only works with one envelope per SystemStreamPartition at a time.
 */
class BatchingChooser(
  wrapped: MessageChooser,
  batchSize: Int = 100,
  metrics: BatchingChooserMetrics = new BatchingChooserMetrics) extends MessageChooser with Logging {

  var preferredSystemStreamPartition: SystemStreamPartition = null
  var preferredEnvelope: IncomingMessageEnvelope = null
  var batchCount = 0

  def update(envelope: IncomingMessageEnvelope) {
    // If we get an envelope for the SSP we're batching, hold on to it so we 
    // can bypass the wrapped chooser, and forcibly return it when choose is 
    // called.
    if (envelope.getSystemStreamPartition.equals(preferredSystemStreamPartition)) {
      debug("Bypassing wrapped.update to cache preferred envelope: %s" format preferredEnvelope)

      preferredEnvelope = envelope
    } else {
      trace("No preferred envelope, so updating wrapped chooser.")

      wrapped.update(envelope)
    }
  }

  /**
   * Chooses the envelope for the SSP that's currently being batched. If no
   * SSP is currently being batched, then this method falls back to calling
   * MessageChooser.choose on the wrapped MessageChooser. If the wrapped
   * MessageChooser returns a non-null envelope, then the SSP for this envelope
   * will become the new batched SSP, and BatchingChooser will choose envelopes
   * for this SSP as long as they're available.
   */
  def choose = {
    if (preferredEnvelope == null) {
      val envelope = wrapped.choose

      // Change preferred SSP to the envelope's SSP, so we can keep batching 
      // on this SSP (as long as an envelope is available).
      if (envelope != null) {
        setPreferredSystemStreamPartition(envelope)
      }

      envelope
    } else {
      val envelope = preferredEnvelope
      preferredEnvelope = null
      batchCount += 1

      trace("Have preferred envelope: %s" format envelope)

      // If we've hit our batching threshold, reset the batch to give other 
      // SSPs a chance to get picked by the wrapped chooser.
      if (batchCount >= batchSize) {
        resetBatch
      }

      envelope
    }
  }

  private def setPreferredSystemStreamPartition(envelope: IncomingMessageEnvelope) {
    debug("Setting preferred system stream partition to: %s" format envelope.getSystemStreamPartition)

    // Set batch count to 1 since the envelope we're returning is the 
    // first one in the batch.
    batchCount = 1
    preferredSystemStreamPartition = envelope.getSystemStreamPartition
  }

  private def resetBatch() {
    debug("Resetting batch due to max batch size limit of: %s" format batchSize)
    metrics.batches.inc
    batchCount = 0
    preferredSystemStreamPartition = null
  }

  def start = {
    debug("Starting batching chooser with batch size of: %s" format batchSize)
    metrics.setBatchedEnvelopes(() => batchCount)
    wrapped.start
  }

  def stop = wrapped.stop

  def register(systemStreamPartition: SystemStreamPartition, offset: String) = wrapped.register(systemStreamPartition, offset)
}

class BatchingChooserMetrics(val registry: MetricsRegistry = new MetricsRegistryMap) extends MetricsHelper {
  val batches = newCounter("batch-resets")

  def setBatchedEnvelopes(getValue: () => Int) {
    newGauge("batched-envelopes", getValue)
  }
}