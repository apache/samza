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
import java.util.concurrent.{TimeUnit, ConcurrentHashMap, Future}

import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.PartitionInfo
import org.apache.samza.SamzaException
import org.apache.samza.system.OutgoingMessageEnvelope
import org.apache.samza.system.SystemProducer
import org.apache.samza.util.ExponentialSleepStrategy
import org.apache.samza.util.KafkaUtil
import org.apache.samza.util.Logging
import org.apache.samza.util.TimerUtils

import scala.collection.JavaConverters._

class KafkaSystemProducer(systemName: String,
                          retryBackoff: ExponentialSleepStrategy = new ExponentialSleepStrategy,
                          getProducer: () => Producer[Array[Byte], Array[Byte]],
                          metrics: KafkaSystemProducerMetrics,
                          val clock: () => Long = () => System.nanoTime) extends SystemProducer with Logging with TimerUtils
{

  class SourceData {
    /**
     * lock to make send() and store its future atomic
     */
    val sendLock: Object = new Object
    /**
     * The most recent send's Future handle
     */
    @volatile
    var latestFuture: Future[RecordMetadata] = null
    /**
     * exceptionInCallback: to store the exception in case of any "ultimate" send failure (ie. failure
     * after exhausting max_retries in Kafka producer) in the I/O thread, we do not continue to queue up more send
     * requests from the samza thread. It helps the samza thread identify if the failure happened in I/O thread or not.
     *
     * In cases of multiple exceptions in the callbacks, we keep the first one before throwing.
     */
    var exceptionInCallback: AtomicReference[SamzaException] = new AtomicReference[SamzaException]()
  }

  @volatile var producer: Producer[Array[Byte], Array[Byte]] = null
  var producerLock: Object = new Object
  val StreamNameNullOrEmptyErrorMsg = "Stream Name should be specified in the stream configuration file."
  val sources: ConcurrentHashMap[String, SourceData] = new ConcurrentHashMap[String, SourceData]

  def start(): Unit = {
  }

  def stop() {
    try {
      val currentProducer = producer
      if (currentProducer != null) {
        producerLock.synchronized {
          if (currentProducer == producer) {
            // only nullify the member producer if it is still the same object, no point nullifying new producer
            producer = null
          }
        }
        currentProducer.close

        sources.asScala.foreach {p =>
          if (p._2.exceptionInCallback.get() == null) {
            flush(p._1)
          }
        }
      }
    } catch {
      case e: Exception => error(e.getMessage, e)
    }
  }

  def register(source: String) {
    if(sources.putIfAbsent(source, new SourceData) != null) {
      throw new SamzaException("%s is already registered with the %s system producer" format (source, systemName))
    }
  }

  def closeAndNullifyCurrentProducer(currentProducer: Producer[Array[Byte], Array[Byte]]) {
    try {
      // TODO: we should use timeout close() to make sure we fail all waiting messages in kafka 0.9+
      currentProducer.close()
    } catch {
      case e: Exception => error("producer close failed", e)
    }
    producerLock.synchronized {
      if (currentProducer == producer) {
        // only nullify the member producer if it is still the same object, no point nullifying new producer
        producer = null
      }
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

    val exception = sourceData.exceptionInCallback.getAndSet(null)
    if (exception != null) {
      metrics.sendFailed.inc
      throw exception  // in case the caller catches all exceptions and will try again
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
      throw new SamzaException("Kafka system producer is not available.")
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
      sourceData.sendLock.synchronized {
        val futureRef: Future[RecordMetadata] =
          currentProducer.send(record, new Callback {
            def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
              if (exception == null) {
                //send was successful.
                metrics.sendSuccess.inc
              }
              else {
                error("Closing the producer because of an exception in callback: ", exception)
                //If there is an exception in the callback, close producer.
                closeAndNullifyCurrentProducer(currentProducer)

                // we keep the exception and will throw the exception in the next producer.send()
                // so the user can handle the exception and decide to fail or ignore
                sourceData.exceptionInCallback.compareAndSet(
                  null,
                  new SamzaException("Unable to send message from %s to system %s." format(source, systemName),
                    exception))

                metrics.sendFailed.inc
                error("Unable to send message on Topic:%s Partition:%s" format(topicName, partitionKey),
                             exception)
              }
            }
          })
        sourceData.latestFuture = futureRef
      }
      metrics.sends.inc
    } catch {
      case e: Exception => {
        error("Closing the producer because of an exception in send: ", e)

        closeAndNullifyCurrentProducer(currentProducer)

        metrics.sendFailed.inc
        throw new SamzaException(("Failed to send message on Topic:%s Partition:%s Exception:\n %s,")
          .format(topicName, partitionKey, e))
      }
    }
  }

  def flush(source: String) {
    updateTimer(metrics.flushNs) {
      metrics.flushes.inc

      val sourceData = sources.get(source)
      //if latestFuture is null, it probably means that there has been no calls to "send" messages
      //Hence, nothing to do in flush
      if(sourceData.latestFuture != null) {
        while(!sourceData.latestFuture.isDone && sourceData.exceptionInCallback.get() == null) {
          try {
            sourceData.latestFuture.get()
          } catch {
            case t: Throwable => error(t.getMessage, t)
          }
        }

        //if there is an exception thrown from the previous callbacks just before flush, we have to fail the container
        if (sourceData.exceptionInCallback.get() != null) {
          metrics.flushFailed.inc
          throw sourceData.exceptionInCallback.get()
        } else {
          trace("Flushed %s." format (source))
        }
      }
    }
  }
}