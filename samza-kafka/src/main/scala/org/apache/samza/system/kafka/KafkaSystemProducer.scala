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

import org.apache.samza.util.Logging
import org.apache.kafka.clients.producer.{RecordMetadata, Callback, ProducerRecord, Producer}
import org.apache.samza.system.SystemProducer
import org.apache.samza.system.OutgoingMessageEnvelope
import org.apache.samza.util.ExponentialSleepStrategy
import org.apache.samza.util.TimerUtils
import org.apache.samza.util.KafkaUtil
import java.util.concurrent.atomic.{AtomicReference, AtomicBoolean}
import java.util.{Map => javaMap}
import org.apache.samza.SamzaException
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.PartitionInfo
import java.util
import java.util.concurrent.Future
import scala.collection.JavaConversions._


class KafkaSystemProducer(systemName: String,
                          retryBackoff: ExponentialSleepStrategy = new ExponentialSleepStrategy,
                          getProducer: () => Producer[Array[Byte], Array[Byte]],
                          metrics: KafkaSystemProducerMetrics,
                          val clock: () => Long = () => System.nanoTime) extends SystemProducer with Logging with TimerUtils
{
  var producer: Producer[Array[Byte], Array[Byte]] = null
  val latestFuture: javaMap[String, Future[RecordMetadata]] = new util.HashMap[String, Future[RecordMetadata]]()
  val sendFailed: AtomicBoolean = new AtomicBoolean(false)
  var exceptionThrown: AtomicReference[Exception] = new AtomicReference[Exception]()
  val StreamNameNullOrEmptyErrorMsg = "Stream Name should be specified in the stream configuration file.";

  def start() {
  }

  def stop() {
    if (producer != null) {
      latestFuture.keys.foreach(flush(_))
      producer.close
      producer = null
    }
  }

  def register(source: String) {
    if(latestFuture.containsKey(source)) {
      throw new SamzaException("%s is already registered with the %s system producer" format (source, systemName))
    }
    latestFuture.put(source, null)
  }

  def send(source: String, envelope: OutgoingMessageEnvelope) {
    trace("Enqueueing message: %s, %s." format (source, envelope))
    if(producer == null) {
      info("Creating a new producer for system %s." format systemName)
      producer = getProducer()
      debug("Created a new producer for system %s." format systemName)
    }
    // Java-based Kafka producer API requires an "Integer" type partitionKey and does not allow custom overriding of Partitioners
    // Any kind of custom partitioning has to be done on the client-side
    val topicName = envelope.getSystemStream.getStream
    if (topicName == null || topicName == "") {
      throw new IllegalArgumentException(StreamNameNullOrEmptyErrorMsg)
    }
    val partitions: java.util.List[PartitionInfo]  = producer.partitionsFor(topicName)
    val partitionKey = if(envelope.getPartitionKey != null) KafkaUtil.getIntegerPartitionKey(envelope, partitions) else null
    val record = new ProducerRecord(envelope.getSystemStream.getStream,
                                    partitionKey,
                                    envelope.getKey.asInstanceOf[Array[Byte]],
                                    envelope.getMessage.asInstanceOf[Array[Byte]])

    sendFailed.set(false)

    retryBackoff.run(
      loop => {
        if(sendFailed.get()) {
          throw exceptionThrown.get()
        }
        val futureRef: Future[RecordMetadata] =
          producer.send(record, new Callback {
            def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
              if (exception == null) {
                //send was successful. Don't retry
                metrics.sendSuccess.inc
              }
              else {
                //If there is an exception in the callback, it means that the Kafka producer has exhausted the max-retries
                //Hence, fail container!
                exceptionThrown.compareAndSet(null, exception)
                sendFailed.set(true)
              }
            }
          })
        latestFuture.put(source, futureRef)
        metrics.sends.inc
        if(!sendFailed.get())
          loop.done
      },
      (exception, loop) => {
        if(exception != null && !exception.isInstanceOf[RetriableException]) {   // Exception is thrown & not retriable
          debug("Exception detail : ", exception)
          //Close producer
          stop()
          producer = null
          //Mark loop as done as we are not going to retry
          loop.done
          metrics.sendFailed.inc
          throw new SamzaException("Failed to send message. Exception:\n %s".format(exception))
        } else {
          warn("Retrying send messsage due to RetriableException - %s. Turn on debugging to get a full stack trace".format(exception))
          debug("Exception detail:", exception)
          metrics.retries.inc
        }
      }
    )
  }

  def flush(source: String) {
    updateTimer(metrics.flushNs) {
      metrics.flushes.inc
      //if latestFuture is null, it probably means that there has been no calls to "send" messages
      //Hence, nothing to do in flush
      if(latestFuture.get(source) != null) {
        while (!latestFuture.get(source).isDone && !sendFailed.get()) {
          //do nothing
        }
        if (sendFailed.get()) {
          logger.error("Unable to send message from %s to system %s" format(source, systemName))
          //Close producer.
          if (producer != null) {
            producer.close
          }
          producer = null
          metrics.flushFailed.inc
          throw new SamzaException("Unable to send message from %s to system %s." format(source, systemName), exceptionThrown.get)
        } else {
          trace("Flushed %s." format (source))
        }
      }
    }
  }
}
