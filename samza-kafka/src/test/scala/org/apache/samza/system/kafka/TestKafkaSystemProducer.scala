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
import org.junit.Assert._
import org.junit.Test
import kafka.producer.Producer
import kafka.producer.async.DefaultEventHandler
import kafka.producer.ProducerPool
import kafka.serializer.Encoder
import java.nio.ByteBuffer
import kafka.producer.ProducerConfig
import java.util.Properties
import scala.collection.JavaConversions._
import kafka.producer.KeyedMessage
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.system.OutgoingMessageEnvelope
import org.apache.samza.system.SystemStream

class TestKafkaSystemProducer {

  val someMessage = new OutgoingMessageEnvelope(new SystemStream("test", "test"), "test")

  def getProps = {
    val props = new Properties
    props.put("broker.list", "")
    props.put("metadata.broker.list", "")
    props
  }

  @Test
  def testKafkaProducer {
    val props = getProps
    @volatile var msgsSent = new CountDownLatch(1)

    val producer = new KafkaSystemProducer("test", 1, 100, new MetricsRegistryMap, () => {
      new Producer[Object, Object](new ProducerConfig(props)) {
        override def send(messages: KeyedMessage[Object, Object]*) {
          msgsSent.countDown
        }
      }
    })

    producer.register("test")
    producer.start
    producer.send("test", someMessage)
    producer.stop
    msgsSent.await(120, TimeUnit.SECONDS)
    assertEquals(0, msgsSent.getCount)
  }

  @Test
  def testKafkaProducerBatch {
    val props = getProps
    @volatile var msgsSent = 0

    val producer = new KafkaSystemProducer("test", 2, 100, new MetricsRegistryMap, () => {
      new Producer[Object, Object](new ProducerConfig(props)) {
        override def send(messages: KeyedMessage[Object, Object]*) {
          msgsSent += 1
        }
      }
    })

    // second message should trigger the count down
    producer.register("test")
    producer.start
    producer.send("test", someMessage)
    assertEquals(0, msgsSent)
    producer.send("test", someMessage)
    assertEquals(1, msgsSent)
    producer.stop
  }

  @Test
  def testKafkaProducerCommit {
    val props = getProps
    val msgs = scala.collection.mutable.ListBuffer[String]()
    val msg1 = new OutgoingMessageEnvelope(new SystemStream("test", "test"), "a")
    val msg2 = new OutgoingMessageEnvelope(new SystemStream("test", "test"), "b")

    val producer = new KafkaSystemProducer("test", 3, 100, new MetricsRegistryMap, () => {
      new Producer[Object, Object](new ProducerConfig(props)) {
        override def send(messages: KeyedMessage[Object, Object]*) {
          msgs ++= messages.map(_.message.asInstanceOf[String])
        }
      }
    })

    // commit should trigger the count down
    producer.register("test")
    producer.start
    producer.send("test", msg1)
    producer.send("test", msg2)
    assertEquals(0, msgs.size)
    producer.commit("test")
    assertEquals(2, msgs.size)
    assertEquals("a", msgs(0))
    assertEquals("b", msgs(1))
    producer.stop
  }

  @Test
  def testKafkaSyncProducerExceptions {
    var msgsSent = 0
    val props = new Properties
    val out = new OutgoingMessageEnvelope(new SystemStream("test", "test"), "a")
    props.put("metadata.broker.list", "")
    props.put("broker.list", "")
    props.put("producer.type", "sync")

    var failCount = 0
    val producer = new KafkaSystemProducer("test", 1, 100, new MetricsRegistryMap, () => {
      failCount += 1
      if (failCount <= 5) {
        throw new RuntimeException("Pretend to fail in factory")
      }
      new Producer[Object, Object](new ProducerConfig(props)) {
        override def send(messages: KeyedMessage[Object, Object]*) {
          assertNotNull(messages)
          assertEquals(1, messages.length)
          assertEquals(messages(0).message, "a")
          msgsSent += 1
          if (msgsSent <= 5) {
            throw new RuntimeException("Pretend to fail in send")
          }
        }
      }
    })

    producer.register("test")
    producer.start
    producer.send("test", out)
    producer.stop
    assertEquals(6, msgsSent)
  }
}
