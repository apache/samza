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

package org.apache.samza.system.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import kafka.producer.ProducerClosedException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.test.TestUtils;

public class MockKafkaProducer implements Producer<byte[], byte[]> {

  private Cluster _cluster;
  private List<FutureTask<RecordMetadata>> _callbacksList = new ArrayList<FutureTask<RecordMetadata>>();
  private boolean shouldBuffer = false;
  private boolean errorNext = false;
  private Exception exception = null;
  private AtomicInteger msgsSent = new AtomicInteger(0);
  private boolean closed = false;

  /*
   * Helps mock out buffered behavior seen in KafkaProducer. This MockKafkaProducer enables you to:
   *  - Create send that will instantly succeed & return a successful future
   *  - Set error for the next message that is sent (using errorNext). In this case, the next call to send returns a
   *    future with exception.
   *    Please note that errorNext is reset to false once a message send has failed. This means that errorNext has to be
   *    manually set to true in the unit test, before expecting failure for another message.
   *  - "shouldBuffer" can be turned on to start buffering messages. This will store all the callbacks and execute it
   *    at a later point of time in a separate thread. This thread NEEDS to be triggered from the unit test itself
   *    using "startDelayedSendThread" method
   *  - "Offset" in RecordMetadata is not guranteed to be correct
   */
  public MockKafkaProducer(int numNodes, String topicName, int numPartitions) {
    this._cluster = TestUtils.clusterWith(numNodes, topicName, numPartitions);
  }

  public void setShouldBuffer(boolean shouldBuffer) {
    this.shouldBuffer = shouldBuffer;
  }

  public void setErrorNext(boolean errorNext, Exception exception) {
    this.errorNext = errorNext;
    this.exception = exception;
  }

  public int getMsgsSent() {
    return this.msgsSent.get();
  }

  public Thread startDelayedSendThread(final int sleepTime) {
    Thread t = new Thread(new FlushRunnable(sleepTime));
    t.start();
    return t;
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord record) {
    return send(record, null);
  }

  private RecordMetadata getRecordMetadata(ProducerRecord record) {
    return new RecordMetadata(new TopicPartition(record.topic(), record.partition() == null ? 0 : record.partition()), 0, this.msgsSent.get(), Record.NO_TIMESTAMP, -1, -1, -1);
  }

  @Override
  public Future<RecordMetadata> send(final ProducerRecord record, final Callback callback) {
    if (closed) {
      throw new ProducerClosedException();
    }
    if (errorNext) {
      if (shouldBuffer) {
        FutureTask<RecordMetadata> f = new FutureTask<RecordMetadata>(new Callable<RecordMetadata>() {
          @Override
          public RecordMetadata call()
              throws Exception {
            callback.onCompletion(null, exception);
            return getRecordMetadata(record);
          }
        });
        _callbacksList.add(f);
        this.errorNext = false;
        return f;
      } else {
        callback.onCompletion(null, this.exception);
        this.errorNext = false;
        return new FutureFailure(this.exception);
      }
    } else {
      if (shouldBuffer) {
        FutureTask<RecordMetadata> f = new FutureTask<RecordMetadata>(new Callable<RecordMetadata>() {
          @Override
          public RecordMetadata call()
              throws Exception {
            msgsSent.incrementAndGet();
            RecordMetadata metadata = getRecordMetadata(record);
            callback.onCompletion(metadata, null);
            return metadata;
          }
        });
        _callbacksList.add(f);
        return f;
      } else {
        int offset = msgsSent.incrementAndGet();
        final RecordMetadata metadata = getRecordMetadata(record);
        callback.onCompletion(metadata, null);
        return new FutureSuccess(record, offset);
      }
    }
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    return this._cluster.partitionsForTopic(topic);
  }

  @Override
  public Map<MetricName, Metric> metrics() {
    return null;
  }

  @Override
  public void close() {
    close(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
  }

  @Override
  public void close(long timeout, TimeUnit timeUnit) {
    closed = true;
    // The real producer will flush messages as part of closing. We'll invoke flush here to approximate that behavior.
    new FlushRunnable(0).run();
  }

  public void open() {
    this.closed = false;
  }

  public boolean isClosed() {
    return closed;
  }

  public synchronized void flush () {
    new FlushRunnable(0).run();
  }


  private static class FutureFailure implements Future<RecordMetadata> {

    private final ExecutionException exception;

    public FutureFailure(Exception exception) {
      this.exception = new ExecutionException(exception);
    }

    @Override
    public boolean cancel(boolean interrupt) {
      return false;
    }

    @Override
    public RecordMetadata get() throws ExecutionException {
      throw this.exception;
    }

    @Override
    public RecordMetadata get(long timeout, TimeUnit unit) throws ExecutionException {
      throw this.exception;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return true;
    }
  }

  private static class FutureSuccess implements Future<RecordMetadata> {

    private ProducerRecord record;
    private final RecordMetadata _metadata;

    public FutureSuccess(ProducerRecord record, int offset) {
      this.record = record;
      this._metadata = new RecordMetadata(new TopicPartition(record.topic(), record.partition() == null ? 0 : record.partition()), 0, offset, Record.NO_TIMESTAMP, -1, -1, -1);
    }

    @Override
    public boolean cancel(boolean interrupt) {
      return false;
    }

    @Override
    public RecordMetadata get() throws ExecutionException {
      return this._metadata;
    }

    @Override
    public RecordMetadata get(long timeout, TimeUnit unit) throws ExecutionException {
      return this._metadata;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return true;
    }
  }

  private class FlushRunnable implements Runnable {
    private final int _sleepTime;

    public FlushRunnable(int sleepTime) {
      _sleepTime = sleepTime;
    }

    public void run() {
      FutureTask[] callbackArray = new FutureTask[_callbacksList.size()];
      AtomicReferenceArray<FutureTask> _bufferList =
          new AtomicReferenceArray<FutureTask>(_callbacksList.toArray(callbackArray));
      ExecutorService executor = Executors.newFixedThreadPool(10);
      try {
        for (int i = 0; i < _bufferList.length(); i++) {
          Thread.sleep(_sleepTime);
          FutureTask f = _bufferList.get(i);
          if (!f.isDone()) {
            executor.submit(f).get();
          }
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException ee) {
        ee.printStackTrace();
      } finally {
        executor.shutdownNow();
      }
    }
  }
}
