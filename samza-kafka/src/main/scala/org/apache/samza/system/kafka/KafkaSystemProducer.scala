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
import org.apache.kafka.common.errors.SerializationException
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
  @volatile var producer: Producer[Array[Byte], Array[Byte]] = null
  val producerLock: Object = new Object

  def start(): Unit = {
    producer = getProducer()
  }

  def stop() {
    info("Stopping producer for system: " + this.systemName)

    // stop() should not happen often so no need to optimize locking
    producerLock.synchronized {
      try {
        if (producer != null) {
          producer.close  // Also performs the equivalent of a flush()
        }

        val exception = fatalException.get()
        if (exception != null) {
          error("Observed an earlier send() error while closing producer", exception)
        }
      } catch {
        case e: Exception => error("Error while closing producer for system: " + systemName, e)
      } finally {
        producer = null
      }
    }
  }

  def register(source: String) {
  }

  def send(source: String, envelope: OutgoingMessageEnvelope) {
    trace("Enqueuing message: %s, %s." format (source, envelope))

    val topicName = envelope.getSystemStream.getStream
    if (topicName == null || topicName == "") {
      throw new IllegalArgumentException("Invalid system stream: " + envelope.getSystemStream)
    }

    val globalProducerException = fatalException.get()
    if (globalProducerException != null) {
      metrics.sendFailed.inc
      throw new SystemProducerException("Producer was unable to recover from previous exception.", globalProducerException)
    }

    val currentProducer = producer
    if (currentProducer == null) {
      throw new SystemProducerException("Kafka producer is null.")
    }

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

            handleSendException(currentProducer, producerException, true)
          }
        }
      })
      metrics.sends.inc
    } catch {
      case originalException : Exception =>
        val producerException = new SystemProducerException("Failed to send message for Source: %s on System:%s Topic:%s Partition:%s"
          .format(source, systemName, topicName, partitionKey), originalException)

        handleSendException(currentProducer, producerException, isFatalException(originalException))
        throw producerException
    }
  }


  def flush(source: String) {
    updateTimer(metrics.flushNs) {
      metrics.flushes.inc

      val currentProducer = producer
      if (currentProducer == null) {
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


  private def handleSendException(currentProducer: Producer[Array[Byte], Array[Byte]], producerException: SystemProducerException, isFatalException: Boolean) = {
    metrics.sendFailed.inc
    error(producerException)
    // The SystemProducer API is synchronous, so there's no way for us to guarantee that an exception will
    // be handled by the Task before we recreate the producer, and if it isn't handled, a concurrent send() from another
    // Task could send on the new producer before the first Task properly handled the exception and produce out of order messages.
    // So we have to handle it right here in the SystemProducer.
    if (dropProducerExceptions) {
      warn("Ignoring producer exception. All messages in the failed producer request will be dropped!")

      if (isFatalException) {
        producerLock.synchronized {
          // Prevent each callback from recreating producer for the same failure.
          if (currentProducer == producer) {
            info("Creating a new producer for system %s." format systemName)
            try {
              currentProducer.close(0, TimeUnit.MILLISECONDS)
            } catch {
              case _: Exception => error("Exception while closing producer.", _)
            }
            producer = getProducer()
          }
        }
      }
    } else {
      // If there is an exception in the callback, it means that the Kafka producer has exhausted the max-retries
      // Close producer to ensure messages queued in-flight are not sent and hence, avoid re-ordering
      // This works because there is only 1 callback thread and no sends can complete until the callback returns.
      if (isFatalException) {
        fatalException.compareAndSet(null, producerException)
        try {
          currentProducer.close(0, TimeUnit.MILLISECONDS)
        } catch {
          case _: Exception => error("Exception while closing producer.", _)
        }
      }
    }
  }

  /**
    * A fatal exception is one that corrupts the producer or otherwise makes it unusable.
    * We want to handle non-fatal exceptions differently because they can often be handled by the user
    * and that's preferable because it gives users that drop exceptions a way to do that with less
    * data loss (no collateral damage from batches of messages getting dropped)
    *
    * @param exception  the exception to check
    * @return           true if the exception is unrecoverable.
    */
  private def isFatalException(exception: Exception): Boolean = {
    exception match {
      case _: SerializationException => false
      case _: ClassCastException => false
      case _ => true
    }
  }
}