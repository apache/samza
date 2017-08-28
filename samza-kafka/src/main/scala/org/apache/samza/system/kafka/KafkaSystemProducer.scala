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
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import org.apache.kafka.clients.producer.{Callback, Producer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.errors.SerializationException
import org.apache.samza.system.{OutgoingMessageEnvelope, SystemProducer, SystemProducerException}
import org.apache.samza.util.{ExponentialSleepStrategy, KafkaUtil, Logging, TimerUtils}

import scala.collection.JavaConverters._

class KafkaSystemProducer(systemName: String,
                          retryBackoff: ExponentialSleepStrategy = new ExponentialSleepStrategy,
                          getProducer: () => Producer[Array[Byte], Array[Byte]],
                          metrics: KafkaSystemProducerMetrics,
                          val clock: () => Long = () => System.nanoTime) extends SystemProducer with Logging with TimerUtils
{

  class SourceData {
    /**
     * producerException: to store the exception in case of any "ultimate" send failure (ie. failure
     * after exhausting max_retries in Kafka producer) in the I/O thread, we do not continue to queue up more send
     * requests from the samza thread. It helps the samza thread identify if the failure happened in I/O thread or not.
     *
     * In cases of multiple exceptions in the callbacks, we keep the first one before throwing.
     */
    val producerException: AtomicReference[SystemProducerException] = new AtomicReference[SystemProducerException]()
  }

  @volatile var producer: Producer[Array[Byte], Array[Byte]] = null
  var producerLock: Object = new Object
  val StreamNameNullOrEmptyErrorMsg = "Stream Name should be specified in the stream configuration file."
  val sources: ConcurrentHashMap[String, SourceData] = new ConcurrentHashMap[String, SourceData]

  def start(): Unit = {
  }

  def stop() {
    info("Stopping producer for system: " + this.systemName)
    try {
      val currentProducer = producer
      if (currentProducer != null) {
        producerLock.synchronized {
          sources.asScala.foreach { p =>
            flush(p._1)
          }

          if (currentProducer == producer) {
            // only nullify the member producer if it is still the same object, no point nullifying new producer
            producer = null
          }

          currentProducer.close
        }
      }
    } catch {
      case e: Exception => error(e.getMessage, e)
    }
  }

  def register(source: String) {
    if(sources.putIfAbsent(source, new SourceData) != null) {
      throw new SystemProducerException("%s is already registered with the %s system producer" format (source, systemName))
    }
  }

  def send(source: String, envelope: OutgoingMessageEnvelope) {
    trace("Enqueuing message: %s, %s." format (source, envelope))

    val topicName = envelope.getSystemStream.getStream
    if (topicName == null || topicName == "") {
      throw new IllegalArgumentException(StreamNameNullOrEmptyErrorMsg)
    }

    val sourceData = sources.get(source)
    if (sourceData == null) {
      throw new IllegalArgumentException("Source %s must be registered first before send." format source)
    }

    // lazy initialization of the producer
    if (producer == null) {
      producerLock.synchronized {
        if (producer == null) {
          info("Creating a new producer for system %s." format systemName)
          producer = getProducer()
        }
      }
    }

    val currentProducer = producer
    if (currentProducer == null) {
      throw new SystemProducerException("Kafka system producer is not available.")
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
            //send was successful.
            metrics.sendSuccess.inc
          }
          else {
            val producerException = new SystemProducerException("Unable to send message from %s to system %s." format(source, systemName),
                exception)

            closeWithException(currentProducer, sourceData, producerException)
            metrics.sendFailed.inc
            error("Unable to send message on Topic:%s Partition:%s" format(topicName, partitionKey),
              exception)
          }
        }
      })
      metrics.sends.inc
    } catch {
      case e: SerializationException => {
        metrics.sendFailed.inc
        throw e
      } case e: Exception => {
        metrics.sendFailed.inc
        error("Closing the producer because of an exception in send: ", e)

        val exception = new SystemProducerException("Failed to send message on Topic:%s Partition:%s"
          .format(topicName, partitionKey), e)

        closeWithException(currentProducer, sourceData, exception)
        throw exception
      }
    }
  }

  def flush(source: String) {
    updateTimer(metrics.flushNs) {
      metrics.flushes.inc

      val currentProducer = producer
      if (currentProducer != null) {
        currentProducer.flush()
      }

      // We must check for an exception AFTER flush() because when flush() returns all callbacks for messages sent
      // in that flush() are guaranteed to have completed and we update the exception in the callback.
      // If there is an exception, we rethrow it here to prevent the checkpoint.
      val exception = sources.get(source).producerException.getAndSet(null)
      if (exception != null) {
        metrics.flushFailed.inc
        producerLock.synchronized {
          producer = null // Want next send() to recreate producer.
        }
        throw new SystemProducerException("Flush failed. A batch of messages were not sent!", exception)
      }
      trace("Flushed %s." format (source))
    }
  }

  // Couples closing the producer with recording the exception.
  // Necessary because there is no isClosed() method to determine whether a new producer is needed. So rely on exception.
  private def closeWithException(producer: Producer[Array[Byte], Array[Byte]], sourceData: SourceData, exception: SystemProducerException): Unit = {
    // If there is an exception in the callback, it means that the Kafka producer has exhausted the max-retries
    // Close producer to ensure messages queued in-flight are not sent and hence, avoid re-ordering
    // This works because the callback thread is singular and no sends can complete until the callback returns.
    producer.close(0, TimeUnit.MILLISECONDS)
    sourceData.producerException.compareAndSet(null, exception)
  }
}