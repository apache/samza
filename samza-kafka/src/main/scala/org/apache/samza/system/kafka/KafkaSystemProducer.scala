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

package org.apache.samza.system.kafka


import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.PartitionInfo
import org.apache.samza.system.OutgoingMessageEnvelope
import org.apache.samza.system.SystemProducer
import org.apache.samza.system.SystemProducerException
import org.apache.samza.util.ExponentialSleepStrategy
import org.apache.samza.util.KafkaUtil
import org.apache.samza.util.Logging
import org.apache.samza.util.TimerUtils

class KafkaSystemProducer(systemName: String,
                          retryBackoff: ExponentialSleepStrategy = new ExponentialSleepStrategy,
                          getProducer: () => Producer[Array[Byte], Array[Byte]],
                          metrics: KafkaSystemProducerMetrics,
                          val clock: () => Long = () => System.nanoTime,
                          val dropProducerExceptions: Boolean = false) extends SystemProducer with Logging with TimerUtils {

  // Represents a fatal error that caused the producer to close.
  val fatalException: AtomicReference[SystemProducerException] = new AtomicReference[SystemProducerException]()
  val producerRef: AtomicReference[Producer[Array[Byte], Array[Byte]]] = new AtomicReference[Producer[Array[Byte], Array[Byte]]]()
  val producerCreationLock: Object = new Object
  @volatile var stopped = false

  def start(): Unit = {
    producerRef.set(getProducer())
  }

  def stop() {
    info("Stopping producer for system: " + this.systemName)

    stopped = true
    val currentProducer = producerRef.getAndSet(null)
    try {
      if (currentProducer != null) {
        currentProducer.close // Also performs the equivalent of a flush()
      }

      val exception = fatalException.get()
      if (exception != null) {
        error("Observed an earlier send() error while closing producer", exception)
      }
    } catch {
      case e: Exception => error("Error while closing producer for system: " + systemName, e)
    }
  }

  def register(source: String) {
  }

  def send(source: String, envelope: OutgoingMessageEnvelope) {
    trace("Enqueuing message: %s, %s." format (source, envelope))

    val topicName = envelope.getSystemStream.getStream
    if (topicName == null || topicName.isEmpty) {
      throw new IllegalArgumentException("Invalid system stream: " + envelope.getSystemStream)
    }

    val globalProducerException = fatalException.get()
    if (globalProducerException != null) {
      metrics.sendFailed.inc
      throw new SystemProducerException("Producer was unable to recover from previous exception.", globalProducerException)
    }

    val currentProducer = getOrCreateCurrentProducer

    // Java-based Kafka producer API requires an "Integer" type partitionKey and does not allow custom overriding of Partitioners
    // Any kind of custom partitioning has to be done on the client-side
    val partitions: java.util.List[PartitionInfo] = currentProducer.partitionsFor(topicName)
    val partitionKey = if (envelope.getPartitionKey != null) KafkaUtil.getIntegerPartitionKey(envelope, partitions) else null
    val record = new ProducerRecord(envelope.getSystemStream.getStream,
                                    partitionKey,
                                    envelope.getKey.asInstanceOf[Array[Byte]],
                                    envelope.getMessage.asInstanceOf[Array[Byte]])

    try {
      currentProducer.send(record, new Callback {
        def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (exception == null) {
            metrics.sendSuccess.inc
          } else {
            val producerException = new SystemProducerException("Failed to send message for Source: %s on System:%s Topic:%s Partition:%s"
              .format(source, systemName, topicName, partitionKey), exception)

            handleFatalSendException(currentProducer, producerException)
          }
        }
      })
      metrics.sends.inc
    } catch {
      case originalException : Exception =>
        val producerException = new SystemProducerException("Failed to send message for Source: %s on System:%s Topic:%s Partition:%s"
          .format(source, systemName, topicName, partitionKey), originalException)

        metrics.sendFailed.inc
        error("Got a synchronous error from Kafka producer.", producerException)
        // Synchronous exceptions are always recoverable so propagate it up and let the user decide
        throw producerException
    }
  }

  def flush(source: String) {
    updateTimer(metrics.flushNs) {
      metrics.flushes.inc

      val currentProducer = producerRef.get()
      if (currentProducer == null) {
        if (dropProducerExceptions) {
          // No producer to flush, but we're ignoring exceptions so just return.
          metrics.flushFailed.inc
          return
        }
        throw new SystemProducerException("Kafka producer is null.")
      }

      // Flush only throws InterruptedException, all other errors are handled in send() callbacks
      currentProducer.flush()

      // Invariant: At this point either
      // 1. The producer is fine and there are no exceptions to handle   OR
      // 2. The producer is closed and one or more sources have exceptions to handle
      //   2a. All new sends get a ProducerClosedException or IllegalStateException (depending on kafka version)
      //   2b. There are no messages in flight because the producer is closed

      // We must check for an exception AFTER flush() because when flush() returns all callbacks for messages sent
      // in that flush() are guaranteed to have completed and we update the exception in the callback.
      // If there is an exception, we rethrow it here to prevent the checkpoint.
      val exception = fatalException.get()
      if (exception != null) {
        metrics.flushFailed.inc
        throw new SystemProducerException("Flush failed. One or more batches of messages were not sent!", exception)
      }
      trace("Flushed %s." format source)
    }
  }


  /**
    * Handles a fatal exception by closing the producer and either recreating it or storing the exception
    * to rethrow later, depending on the value of dropProducerExceptions.
    *
    * @param currentProducer   the current producer for which the exception occurred. Must not be null.
    * @param producerException the exception to handle.
    */
  private def handleFatalSendException(currentProducer: Producer[Array[Byte], Array[Byte]], producerException: SystemProducerException): Unit = {
    metrics.sendFailed.inc
    error(producerException)
    // The SystemProducer API is synchronous, so there's no way for us to guarantee that an exception will
    // be handled by the Task before we recreate the producer, and if it isn't handled, a concurrent send() from another
    // Task could send on the new producer before the first Task properly handled the exception and produce out of order messages.
    // So we have to handle it right here in the SystemProducer.
    if (dropProducerExceptions) {
      warn("Ignoring producer exception. All messages in the failed producer request will be dropped!")

      // Prevent each callback from closing and nulling producer for the same failure.
      if (currentProducer == producerRef.get()) {
        info("Closing producer for system %s." format systemName)
        try {
          // send()s can get ProducerClosedException if the producer is stopped after they get the currentProducer
          // reference but before producer.send() returns. That's ONLY ok when dropProducerExceptions is true.
          // Also, when producer.close(0) is invoked on the Kafka IO thread, when it returns there will be no more
          // messages sent over the wire. This is key to ensuring no out-of-order messages as a result of recreating
          // the producer.
          currentProducer.close(0, TimeUnit.MILLISECONDS)
        } catch {
          case exception: Exception => error("Exception while closing producer.", exception)
        }
        producerRef.compareAndSet(currentProducer, null)
      }
    } else {
      // If there is an exception in the callback, it means that the Kafka producer has exhausted the max-retries
      // Close producer to ensure messages queued in-flight are not sent and hence, avoid re-ordering
      // This works because there is only 1 IO thread and no IO can be done until the callback returns.
      // Do not create a new producer here! It cannot be done without data loss for all concurrency modes.
      fatalException.compareAndSet(null, producerException)
      try {
        currentProducer.close(0, TimeUnit.MILLISECONDS)
      } catch {
        case exception: Exception => error("Exception while closing producer.", exception)
      }
    }
  }

  /**
    * @return the current producer. Never returns null.
    */
  private def getOrCreateCurrentProducer = {
    var currentProducer = producerRef.get

    if (currentProducer == null) {
      if (dropProducerExceptions && !stopped) {
        // Note: While this lock prevents others from creating a new producer, they could still set it to null.
        producerCreationLock.synchronized {
          currentProducer = producerRef.get
          if (currentProducer == null) {
            currentProducer = getProducer()
            producerRef.set(currentProducer)
          }
        }
        // Invariant: currentProducer must not be null at this point.
      } else {
        throw new SystemProducerException("Kafka producer is null.")
      }
    }
    currentProducer
  }
}