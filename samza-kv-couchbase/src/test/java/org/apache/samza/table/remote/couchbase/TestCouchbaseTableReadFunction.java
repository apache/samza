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

package org.apache.samza.table.remote.couchbase;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.BinaryDocument;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.couchbase.client.java.error.TemporaryLockFailureException;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.samza.SamzaException;
import org.apache.samza.context.Context;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.table.AsyncReadWriteUpdateTable;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import rx.Observable;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.powermock.api.mockito.PowerMockito.*;


/**
 * This class performs unit tests for CouchbaseTableReadFunction.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(CouchbaseBucketRegistry.class)
public class TestCouchbaseTableReadFunction {
  private static final String DEFAULT_BUCKET_NAME = "default-bucket-name";
  private static final String DEFAULT_CLUSTER_NODE = "localhost";

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorInvalidBucketName() {
    new CouchbaseTableReadFunction<>("", String.class, DEFAULT_CLUSTER_NODE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorInvalidClusterNodes() {
    new CouchbaseTableReadFunction<>(DEFAULT_BUCKET_NAME, String.class);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorInvalidValueClass() {
    new CouchbaseTableReadFunction<>(DEFAULT_BUCKET_NAME, null, DEFAULT_CLUSTER_NODE);
  }

  @Test
  public void testIsNotRetriable() {
    CouchbaseTableReadFunction readFunction = createAndInit();
    assertFalse(readFunction.isRetriable(null));
    assertFalse(readFunction.isRetriable(new SamzaException()));
    assertFalse(readFunction.isRetriable(new SamzaException("", new IllegalArgumentException())));
  }

  @Test
  public void testIsRetriable() {
    CouchbaseTableReadFunction readFunction = createAndInit();
    assertTrue(readFunction.isRetriable(new TemporaryFailureException()));
    assertTrue(readFunction.isRetriable(new TemporaryLockFailureException()));
    assertTrue(readFunction.isRetriable(new SamzaException(new TemporaryLockFailureException())));
    assertTrue(readFunction.isRetriable(new SamzaException(new TemporaryFailureException())));
    assertTrue(readFunction.isRetriable(
        new RuntimeException(new SamzaException(new RuntimeException(new TemporaryFailureException())))));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetAsyncNullKey() {
    CouchbaseTableReadFunction readFunction = createAndInit();
    readFunction.getAsync(null);
  }

  @Test
  public void testGetAsyncException() {
    String key = "throwExceptionKey";
    Bucket bucket = mock(Bucket.class);
    AsyncBucket asyncBucket = mock(AsyncBucket.class);
    CouchbaseTableReadFunction readFunction = createAndInit(bucket, asyncBucket);
    when(asyncBucket.get(eq(key), anyObject(), anyLong(), any(TimeUnit.class))).thenReturn(
        Observable.error(new CouchbaseException()));
    assertTrue(readFunction.getAsync(key).isCompletedExceptionally());
  }

  @Test
  public void testGetAsyncFailedToDeserialize() {
    String key = "key";
    Bucket bucket = mock(Bucket.class);
    AsyncBucket asyncBucket = mock(AsyncBucket.class);
    CouchbaseTableReadFunction readFunction = createAndInit(String.class, new StringSerde(), bucket, asyncBucket);
    when(asyncBucket.get(eq(key), anyObject(), anyLong(), any(TimeUnit.class))).thenReturn(
        Observable.just(BinaryDocument.create(key, null)));
    assertTrue(readFunction.getAsync(key).isCompletedExceptionally());
  }

  @Test
  public void testGetAsyncNullValue() throws Exception {
    String key = "NonExistingKey";
    Bucket bucket = mock(Bucket.class);
    AsyncBucket asyncBucket = mock(AsyncBucket.class);
    CouchbaseTableReadFunction readFunction = createAndInit(String.class, new StringSerde(), bucket, asyncBucket);
    when(asyncBucket.get(eq(key), anyObject(), anyLong(), any(TimeUnit.class))).thenReturn(Observable.empty());
    assertNull(readFunction.getAsync(key).get());
  }

  @Test
  public void testGetAsyncNonExistingKey() throws Exception {
    String key = "NonExistingKey";
    Bucket bucket = mock(Bucket.class);
    AsyncBucket asyncBucket = mock(AsyncBucket.class);
    CouchbaseTableReadFunction readFunction = createAndInit(String.class, new StringSerde(), bucket, asyncBucket);
    when(asyncBucket.get(eq(key), anyObject(), anyLong(), any(TimeUnit.class))).thenReturn(Observable.empty());
    assertNull(readFunction.getAsync(key).get());
  }

  @Test
  public void testGetAsyncStringValue() throws Exception {
    String key = "key";
    String value = "value";
    StringSerde stringSerde = new StringSerde();
    Bucket bucket = mock(Bucket.class);
    AsyncBucket asyncBucket = mock(AsyncBucket.class);
    CouchbaseTableReadFunction readFunction = createAndInit(String.class, stringSerde, bucket, asyncBucket);
    when(asyncBucket.get(eq(key), anyObject(), anyLong(), any(TimeUnit.class))).thenReturn(
        Observable.just(BinaryDocument.create(key, Unpooled.wrappedBuffer(stringSerde.toBytes(value)))));
    assertEquals(value, readFunction.getAsync(key).get());
  }

  @Test
  public void testGetAsyncJsonObjectValue() throws Exception {
    String key = "key";
    JsonObject value = JsonObject.fromJson("{\"id\": 1}");
    Bucket bucket = mock(Bucket.class);
    AsyncBucket asyncBucket = mock(AsyncBucket.class);
    CouchbaseTableReadFunction readFunction = createAndInit(bucket, asyncBucket);
    when(asyncBucket.get(eq(key), anyObject(), anyLong(), any(TimeUnit.class))).thenReturn(
        Observable.just(JsonDocument.create(key, value)));
    assertEquals(value, readFunction.getAsync(key).get());
  }

  private CouchbaseTableReadFunction<JsonObject> createAndInit() {
    return createAndInit(mock(Bucket.class), mock(AsyncBucket.class));
  }

  private CouchbaseTableReadFunction<JsonObject> createAndInit(Bucket bucket, AsyncBucket asyncBucket) {
    return createAndInit(JsonObject.class, null, bucket, asyncBucket);
  }

  private <V> CouchbaseTableReadFunction<V> createAndInit(Class<V> valueClass, Serde<V> serde, Bucket bucket,
      AsyncBucket asyncBucket) {
    when(bucket.async()).thenReturn(asyncBucket);
    PowerMockito.stub(PowerMockito.method(CouchbaseBucketRegistry.class, "getBucket", String.class, List.class,
        CouchbaseEnvironmentConfigs.class)).toReturn(bucket);
    CouchbaseTableReadFunction<V> readFunction =
        new CouchbaseTableReadFunction<>(DEFAULT_BUCKET_NAME, valueClass, DEFAULT_CLUSTER_NODE).withSerde(serde);
    readFunction.init(mock(Context.class), mock(AsyncReadWriteUpdateTable.class));
    return readFunction;
  }
}
