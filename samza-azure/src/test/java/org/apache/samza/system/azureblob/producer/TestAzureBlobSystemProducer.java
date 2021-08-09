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

import org.apache.samza.system.azureblob.AzureBlobBasicMetrics;
import org.apache.samza.system.azureblob.AzureBlobConfig;
import org.apache.samza.system.azureblob.avro.AzureBlobAvroWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducerException;
import org.apache.samza.system.SystemStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({AzureBlobSystemProducer.class, ThreadPoolExecutor.class})
public class TestAzureBlobSystemProducer {

  private final static String SYSTEM_NAME = "FAKE_SYSTEM";
  private final static String SOURCE = "FAKE_SOURCE";
  private final static String STREAM = "FAKE_STREAM";
  private final static String ACCOUNT_NAME = "FAKE_ACCOUNT_NAME";
  private final static String ACCOUNT_KEY = "FAKE_ACCOUNT_KEY";

  private OutgoingMessageEnvelope ome;
  private AzureBlobSystemProducer systemProducer;
  private MetricsRegistry mockMetricsRegistry;
  private AzureBlobWriter mockAzureWriter;
  private ThreadPoolExecutor mockThreadPoolExecutor;
  private Counter mockErrorCounter;
  private boolean exceptionOccured = false;

  @Before
  public void setup() throws Exception {

    AzureBlobConfig azureBlobConfig = new AzureBlobConfig(getBasicConfigs());

    mockMetricsRegistry = mock(MetricsRegistry.class);
    mockErrorCounter = mock(Counter.class);

    when(mockMetricsRegistry.newCounter(anyString(), anyString())).thenReturn(mock(Counter.class));
    when(mockMetricsRegistry.newCounter(SOURCE, AzureBlobBasicMetrics.EVENT_PRODUCE_ERROR)).thenReturn(mockErrorCounter);

    ome = createOME(STREAM);

    mockThreadPoolExecutor = spy(new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS,
        new LinkedBlockingDeque<Runnable>()));

    PowerMockito.whenNew(ThreadPoolExecutor.class).withAnyArguments().thenReturn(mockThreadPoolExecutor);

    mockAzureWriter = mock(AzureBlobWriter.class);
    doNothing().when(mockAzureWriter).close();

    systemProducer = spy(new AzureBlobSystemProducer(SYSTEM_NAME, azureBlobConfig, mockMetricsRegistry));
    // use mock writer impl
    setupWriterForProducer(systemProducer, mockAzureWriter, STREAM);
    // bypass Azure connection setup
    doNothing().when(systemProducer).setupAzureContainer();
  }

  @Test
  public void testStart() {

    systemProducer.start();
    verify(systemProducer).setupAzureContainer();
  }

  public void testMultipleStart() {
    systemProducer.start();
    systemProducer.start();
  }

  @Test
  public void testStop() throws Exception {
    doNothing().when(mockAzureWriter).close();

    systemProducer.register(SOURCE);
    systemProducer.start();
    systemProducer.send(SOURCE, ome);
    systemProducer.flush(SOURCE);
    systemProducer.stop();

    verify(mockAzureWriter).flush(); // called during flush IN STOP
    verify(mockAzureWriter).close(); // called during flush in STOP
  }

  @Test
  public void testStopBeforeFlush() throws Exception {

    systemProducer.register(SOURCE);
    systemProducer.start();
    systemProducer.send(SOURCE, ome);
    systemProducer.stop();

    verify(mockAzureWriter).flush(); // called during flush IN STOP
    verify(mockAzureWriter).close(); // called during flush in STOP
  }

  @Test(expected = SystemProducerException.class)
  public void testStopWhenThreadpoolShutdownFails() throws Exception {
    doThrow(new SecurityException("failed")).when(mockThreadPoolExecutor).shutdown();
    systemProducer.start();
    systemProducer.stop();
  }

  @Test (expected = SystemProducerException.class)
  public void testStopWhenWriterFails() throws IOException {
    doThrow(new SystemProducerException("Failed")).when(mockAzureWriter).flush();
    systemProducer.register(SOURCE);
    systemProducer.start();
    systemProducer.send(SOURCE, ome);
    systemProducer.stop();
  }

  @Test(expected = SystemProducerException.class)
  public void testRegisterAfterStart() throws Exception {
    systemProducer.start();
    systemProducer.register(SOURCE);
  }

  @Test
  public void testRegisterMetrics() throws Exception {
    systemProducer.register(SOURCE);

    // verify that new counter for system was created during constructor of producer
    verify(mockMetricsRegistry).newCounter(
        String.format(AzureBlobSystemProducerMetrics.SYSTEM_METRIC_FORMAT, ACCOUNT_NAME, SYSTEM_NAME),
        AzureBlobBasicMetrics.EVENT_WRITE_RATE);
    // verify that new counter for source was created during register
    verify(mockMetricsRegistry).newCounter(SOURCE, AzureBlobBasicMetrics.EVENT_WRITE_RATE);
    verify(mockMetricsRegistry).newCounter(SOURCE, AzureBlobBasicMetrics.EVENT_WRITE_BYTE_RATE);
    verify(mockMetricsRegistry).newCounter(SOURCE, AzureBlobBasicMetrics.EVENT_PRODUCE_ERROR);
  }

  @Test
  public void testRegisterWithSystemName() throws Exception {
    systemProducer.register(SYSTEM_NAME);

    // verify that new counter for system was created during constructor of producer but not during register
    verify(mockMetricsRegistry).newCounter(
        String.format(AzureBlobSystemProducerMetrics.SYSTEM_METRIC_FORMAT, ACCOUNT_NAME, SYSTEM_NAME),
        AzureBlobBasicMetrics.EVENT_WRITE_RATE);
  }

  @Test
  public void testFlush() throws IOException {

    systemProducer.register(SOURCE);
    systemProducer.start();
    systemProducer.send(SOURCE, ome);
    systemProducer.flush(SOURCE);

    verify(mockAzureWriter).flush(); // called during flush
    verify(mockAzureWriter).close(); // called during flush
  }

  @Test(expected = SystemProducerException.class)
  public void testFlushBeforeStart() throws Exception {
    systemProducer.flush(SOURCE);
  }

  @Test(expected = SystemProducerException.class)
  public void testFlushBeforeRegister() throws Exception {
    systemProducer.start();
    systemProducer.flush(SOURCE);
  }

  @Test
  public void testFlushWhenWriterUploadFails() throws Exception {
    doThrow(new SystemProducerException("failed")).when(mockAzureWriter).flush();

    systemProducer.register(SOURCE);
    systemProducer.start();
    systemProducer.send(SOURCE, ome);
    try {
      systemProducer.flush(SOURCE);
      Assert.fail("Expected exception not thrown.");
    } catch (SystemProducerException e) {
    }

    verify(mockErrorCounter).inc();
  }

  @Test
  public void testFlushWhenWriterCloseFails() throws Exception {
    doThrow(new SystemProducerException("failed")).when(mockAzureWriter).close();

    systemProducer.register(SOURCE);
    systemProducer.start();
    systemProducer.send(SOURCE, ome);
    try {
      systemProducer.flush(SOURCE);
      Assert.fail("Expected exception not thrown.");
    } catch (SystemProducerException e) {
    }
    verify(mockErrorCounter).inc();
  }

  @Test
  public void testSend() throws IOException {
    int numberOfMessages = 10;
    Counter mockWriteCounter = mock(Counter.class);
    when(mockMetricsRegistry.newCounter(SOURCE, AzureBlobBasicMetrics.EVENT_WRITE_RATE)).thenReturn(mockWriteCounter);

    systemProducer.register(SOURCE);
    systemProducer.start();
    for (int i = 0; i < numberOfMessages; i++) {
      systemProducer.send(SOURCE, ome);
    }
    verify(mockAzureWriter, times(numberOfMessages)).write(ome);

    // verify metrics
    verify(mockWriteCounter, times(numberOfMessages)).inc();
  }

  @Test
  public void testSendWhenWriterCreateFails() throws Exception {
    AzureBlobConfig azureBlobConfig = new AzureBlobConfig(getBasicConfigs());

    AzureBlobSystemProducer systemProducer = spy(new AzureBlobSystemProducer(SYSTEM_NAME, azureBlobConfig,
        mockMetricsRegistry));
    PowerMockito.whenNew(AzureBlobAvroWriter.class).withAnyArguments().thenThrow(new SystemProducerException("Failed"));
    // bypass Azure connection setup
    doNothing().when(systemProducer).setupAzureContainer();

    systemProducer.register(SOURCE);
    systemProducer.start();
    try {
      systemProducer.send(SOURCE, ome);
      Assert.fail("Expected exception not thrown.");
    } catch (SystemProducerException e) {
    }
    verify(mockErrorCounter).inc();
  }

  @Test
  public void testSendWhenWriterFails() throws Exception {

    doThrow(new SystemProducerException("failed")).when(mockAzureWriter).write(ome);

    systemProducer.register(SOURCE);
    systemProducer.start();
    try {
      systemProducer.send(SOURCE, ome);
      Assert.fail("Expected exception not thrown.");
    } catch (SystemProducerException e) {
    }
    verify(mockErrorCounter).inc();
  }

  @Test
  public void testMutipleThread() throws Exception {
    String source1 = "FAKE_SOURCE_1";
    String source2 = "FAKE_SOURCE_2";

    String stream1 = "FAKE_STREAM_1";
    String stream2 = "FAKE_STREAM_2";

    int sendsInFirstThread = 10;
    int sendsInSecondThread = 20;

    OutgoingMessageEnvelope ome1 = createOME(stream1);
    OutgoingMessageEnvelope ome2 = createAnotherOME(stream2);

    AzureBlobWriter mockAzureWriter1 = mock(AzureBlobWriter.class);
    doNothing().when(mockAzureWriter1).close();

    AzureBlobWriter mockAzureWriter2 = mock(AzureBlobWriter.class);
    doNothing().when(mockAzureWriter2).close();

    AzureBlobConfig azureBlobConfig = new AzureBlobConfig(getBasicConfigs());

    AzureBlobSystemProducer systemProducer = spy(new AzureBlobSystemProducer(SYSTEM_NAME, azureBlobConfig, mockMetricsRegistry));
    // bypass Azure connection setup
    doNothing().when(systemProducer).setupAzureContainer();

    doReturn(mockAzureWriter1).when(systemProducer).getOrCreateWriter(source1, ome1);
    doReturn(mockAzureWriter2).when(systemProducer).getOrCreateWriter(source2, ome2);

    systemProducer.register(source1);
    systemProducer.register(source2);
    systemProducer.start();
    Thread t1 = sendFlushInThread(source1, ome1, systemProducer, sendsInFirstThread);
    Thread t2 = sendFlushInThread(source2, ome2, systemProducer, sendsInSecondThread);
    t1.start();
    t2.start();
    t1.join(60000);
    t2.join(60000);
    systemProducer.stop();
    verify(mockAzureWriter1, times(sendsInFirstThread)).write(ome1);
    verify(mockAzureWriter2, times(sendsInSecondThread)).write(ome2);
  }

  @Test
  public void testMutipleThreadOneWriterFails() throws Exception {
    String source1 = "FAKE_SOURCE_1";
    String source2 = "FAKE_SOURCE_2";

    String stream1 = "FAKE_STREAM_1";
    String stream2 = "FAKE_STREAM_2";

    int sendsInFirstThread = 10;
    int sendsInSecondThread = 20;

    OutgoingMessageEnvelope ome1 = createOME(stream1);
    OutgoingMessageEnvelope ome2 = createAnotherOME(stream2);

    AzureBlobWriter mockAzureWriter1 = mock(AzureBlobWriter.class);
    doThrow(new SystemProducerException("failed")).when(mockAzureWriter1).write(ome1);
    doNothing().when(mockAzureWriter1).close();

    AzureBlobWriter mockAzureWriter2 = mock(AzureBlobWriter.class);
    doNothing().when(mockAzureWriter2).close();

    AzureBlobConfig azureBlobConfig = new AzureBlobConfig(getBasicConfigs());

    AzureBlobSystemProducer systemProducer = spy(new AzureBlobSystemProducer(SYSTEM_NAME, azureBlobConfig, mockMetricsRegistry));
    // bypass Azure connection setup
    doNothing().when(systemProducer).setupAzureContainer();

    doReturn(mockAzureWriter1).when(systemProducer).getOrCreateWriter(source1, ome1);
    doReturn(mockAzureWriter2).when(systemProducer).getOrCreateWriter(source2, ome2);


    systemProducer.register(source1);
    systemProducer.register(source2);
    systemProducer.start();
    Thread t1 = sendFlushInThread(source1, ome1, systemProducer, sendsInFirstThread);
    Thread t2 = sendFlushInThread(source2, ome2, systemProducer, sendsInSecondThread);

    Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
      public void uncaughtException(Thread th, Throwable ex) {
        if (ex instanceof SystemProducerException) {
          exceptionOccured = true;
        }
      }
    };
    t1.setUncaughtExceptionHandler(handler);
    t1.start();
    t2.start();
    t1.join(60000);
    t2.join(60000);

    systemProducer.stop();

    if (!exceptionOccured) {
      Assert.fail("Expected SystemProducerException but did not occur.");
    }
    verify(mockAzureWriter1).write(ome1);
    verify(mockAzureWriter2, times(sendsInSecondThread)).write(ome2);
  }

  @Test
  public void testMutipleThreadSendFlushToSingleWriter() throws Exception {
    String source1 = "FAKE_SOURCE_1";

    String stream1 = "FAKE_STREAM_1";

    int sendsInFirstThread = 10;
    int sendsInSecondThread = 20;

    OutgoingMessageEnvelope ome1 = createOME(stream1);

    AzureBlobWriter mockAzureWriter1 = mock(AzureBlobWriter.class);
    doNothing().when(mockAzureWriter1).close();

    AzureBlobConfig azureBlobConfig = new AzureBlobConfig(getBasicConfigs());

    AzureBlobSystemProducer systemProducer = spy(new AzureBlobSystemProducer(SYSTEM_NAME, azureBlobConfig, mockMetricsRegistry));
    // bypass Azure connection setup
    doNothing().when(systemProducer).setupAzureContainer();

    systemProducer.register(source1);
    systemProducer.start();

    setupWriterForProducer(systemProducer, mockAzureWriter1, stream1);

    Thread t1 = sendFlushInThread(source1, ome1, systemProducer, sendsInFirstThread);
    Thread t2 = sendFlushInThread(source1, ome1, systemProducer, sendsInSecondThread);

    t1.start();
    t2.start();
    t1.join(60000);
    t2.join(60000);
    systemProducer.stop();
    verify(mockAzureWriter1, times(sendsInFirstThread + sendsInSecondThread)).write(ome1);
    verify(mockAzureWriter1, times(2)).flush();
    verify(mockAzureWriter1, times(2)).close();
  }

  @Test
  public void testMutipleThreadSendToSingleWriter() throws Exception {
    String source1 = "FAKE_SOURCE_1";

    String stream1 = "FAKE_STREAM_1";

    int sendsInFirstThread = 10;
    int sendsInSecondThread = 20;

    OutgoingMessageEnvelope ome1 = createOME(stream1);
    OutgoingMessageEnvelope ome2 = createAnotherOME(stream1);

    AzureBlobWriter mockAzureWriter1 = mock(AzureBlobWriter.class);
    doNothing().when(mockAzureWriter1).close();

    AzureBlobConfig azureBlobConfig = new AzureBlobConfig(getBasicConfigs());

    AzureBlobSystemProducer systemProducer = spy(new AzureBlobSystemProducer(SYSTEM_NAME, azureBlobConfig, mockMetricsRegistry));
    // bypass Azure connection setup
    doNothing().when(systemProducer).setupAzureContainer();

    setupWriterForProducer(systemProducer, mockAzureWriter1, stream1);

    systemProducer.register(source1);
    systemProducer.start();
    Thread t1 = new Thread() {
      @Override
      public void run() {
          for (int i = 0; i < sendsInFirstThread; i++) {
            systemProducer.send(source1, ome1);
          }
      }
    };
    Thread t2 = new Thread() {
      @Override
      public void run() {
          for (int i = 0; i < sendsInSecondThread; i++) {
            systemProducer.send(source1, ome2);
          }
      }
    };
    t1.start();
    t2.start();
    t1.join(60000);
    t2.join(60000);
    systemProducer.stop();
    verify(mockAzureWriter1, times(sendsInFirstThread)).write(ome1);
    verify(mockAzureWriter1, times(sendsInSecondThread)).write(ome2);
  }

  @Test
  public void testMutipleThreadFlushToSingleWriter() throws Exception {
    String source1 = "FAKE_SOURCE_1";

    AzureBlobWriter mockAzureWriter1 = mock(AzureBlobWriter.class);
    doNothing().when(mockAzureWriter1).close();

    AzureBlobConfig azureBlobConfig = new AzureBlobConfig(getBasicConfigs());

    AzureBlobSystemProducer systemProducer = spy(new AzureBlobSystemProducer(SYSTEM_NAME, azureBlobConfig, mockMetricsRegistry));
    // bypass Azure connection setup
    doNothing().when(systemProducer).setupAzureContainer();

    setupWriterForProducer(systemProducer, mockAzureWriter1, STREAM);

    systemProducer.register(source1);
    systemProducer.start();
    systemProducer.send(source1, ome); //to create writer
    Thread t1 = new Thread() {
      @Override
      public void run() {
          systemProducer.flush(source1);
      }
    };
    Thread t2 = new Thread() {
      @Override
      public void run() {
        systemProducer.flush(source1);
      }
    };
    t1.start();
    t2.start();
    t1.join(60000);
    t2.join(60000);
    systemProducer.stop();
    // systemProducer.flush called twice but first flush clears the writer map of the source.
    // hence, writer.flush and close called only once.
    verify(mockAzureWriter1).flush();
    verify(mockAzureWriter1).close();
  }

  private Thread sendFlushInThread(String source, OutgoingMessageEnvelope ome, AzureBlobSystemProducer systemProducer,
      int numberOfSends) {
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          for (int i = 0; i < numberOfSends; i++) {
            systemProducer.send(source, ome);
          }
          systemProducer.flush(source);
        } catch (Exception e) {
          throw e;
        }
      }
    };
    return t;
  }

  private OutgoingMessageEnvelope createOME(String streamName) {
    SystemStream systemStream = new SystemStream(SYSTEM_NAME, streamName);
    DummyPageViewEvent record = new DummyPageViewEvent();
    return new OutgoingMessageEnvelope(systemStream, record);
  }

  private OutgoingMessageEnvelope createAnotherOME(String streamName) {
    SystemStream systemStream = new SystemStream(SYSTEM_NAME, streamName);
    AnotherDummyPageViewEvent record = new AnotherDummyPageViewEvent();
    return new OutgoingMessageEnvelope(systemStream, record);
  }

  private class DummyPageViewEvent extends org.apache.avro.specific.SpecificRecordBase
      implements org.apache.avro.specific.SpecificRecord {
    public final org.apache.avro.Schema schema = org.apache.avro.Schema.parse(
        "{\"type\":\"record\",\"name\":\"DummyPageViewEvent\",\"namespace\":\"org.apache.samza.events\",\"fields\":[]}");

    public org.apache.avro.Schema getSchema() {
      return schema;
    }

    public java.lang.Object get(int field) {
      return null;
    }

    public void put(int field, Object value) {}
  }

  private class AnotherDummyPageViewEvent extends org.apache.avro.specific.SpecificRecordBase
      implements org.apache.avro.specific.SpecificRecord {
    public final org.apache.avro.Schema schema = org.apache.avro.Schema.parse(
        "{\"type\":\"record\",\"name\":\"AnotherDummyPageViewEvent\",\"namespace\":\"org.apache.samza.events\",\"fields\":[]}");

    public org.apache.avro.Schema getSchema() {
      return schema;
    }

    public java.lang.Object get(int field) {
      return null;
    }

    public void put(int field, Object value) {}
  }

  private Config getBasicConfigs() {
    Map<String, String> bareConfigs = new HashMap<>();
    bareConfigs.put(String.format(AzureBlobConfig.SYSTEM_AZURE_ACCOUNT_NAME, SYSTEM_NAME), ACCOUNT_NAME);
    bareConfigs.put(String.format(AzureBlobConfig.SYSTEM_AZURE_ACCOUNT_KEY, SYSTEM_NAME), ACCOUNT_KEY);
    bareConfigs.put(String.format(AzureBlobConfig.SYSTEM_CLOSE_TIMEOUT_MS, SYSTEM_NAME), "1000");
    Config config = new MapConfig(bareConfigs);
    return config;
  }

  private void setupWriterForProducer(AzureBlobSystemProducer azureBlobSystemProducer,
      AzureBlobWriter mockAzureBlobWriter, String stream) {
    doAnswer(invocation -> {
      String blobUrl = invocation.getArgumentAt(0, String.class);
      String streamName = invocation.getArgumentAt(2, String.class);
      Assert.assertEquals(stream, streamName);
      Assert.assertEquals(stream, blobUrl);
      return mockAzureBlobWriter;
    }).when(azureBlobSystemProducer).createNewWriter(anyString(), any(), anyString());
  }
}