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
import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.Document;
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
 * This class performs unit tests for CouchbaseTableWriteFunction.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(CouchbaseBucketRegistry.class)
public class TestCouchbaseTableWriteFunction {
  private static final String DEFAULT_BUCKET_NAME = "default-bucket-name";
  private static final String DEFAULT_CLUSTER_NODE = "localhost";

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorInvalidBucketName() {
    new CouchbaseTableWriteFunction<>("", String.class, DEFAULT_CLUSTER_NODE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorInvalidClusterNodes() {
    new CouchbaseTableWriteFunction<>(DEFAULT_BUCKET_NAME, String.class);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorInvalidValueClass() {
    new CouchbaseTableWriteFunction<>(DEFAULT_BUCKET_NAME, null, DEFAULT_CLUSTER_NODE);
  }

  @Test
  public void testIsNotRetriable() {
    CouchbaseTableWriteFunction writeFunction = createAndInit();
    assertFalse(writeFunction.isRetriable(null));
    assertFalse(writeFunction.isRetriable(new SamzaException()));
    assertFalse(writeFunction.isRetriable(new SamzaException("", new IllegalArgumentException())));
  }

  @Test
  public void testIsRetriable() {
    CouchbaseTableWriteFunction writeFunction = createAndInit();
    assertTrue(writeFunction.isRetriable(new TemporaryFailureException()));
    assertTrue(writeFunction.isRetriable(new TemporaryLockFailureException()));
    assertTrue(writeFunction.isRetriable(new SamzaException(new TemporaryLockFailureException())));
    assertTrue(writeFunction.isRetriable(new SamzaException(new TemporaryFailureException())));
    assertTrue(writeFunction.isRetriable(
        new RuntimeException(new SamzaException(new RuntimeException(new TemporaryFailureException())))));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPutAsyncNullKey() {
    CouchbaseTableWriteFunction<JsonObject> writeFunction = createAndInit();
    writeFunction.putAsync(null, JsonObject.create());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPutAsyncKeyWithSpaces() {
    CouchbaseTableWriteFunction<JsonObject> writeFunction = createAndInit();
    writeFunction.putAsync("k e y", JsonObject.create());
  }

  @Test(expected = NullPointerException.class)
  public void testPutAsyncNullValue() {
    CouchbaseTableWriteFunction<JsonObject> writeFunction = createAndInit();
    writeFunction.putAsync("key", null);
  }

  @Test
  public void testPutAsyncException() {
    String key = "throwExceptionKey";
    JsonObject value = JsonObject.create();
    Bucket bucket = mock(Bucket.class);
    AsyncBucket asyncBucket = mock(AsyncBucket.class);
    CouchbaseTableWriteFunction<JsonObject> writeFunction = createAndInit(bucket, asyncBucket);
    when(asyncBucket.upsert(any(Document.class), anyLong(), any(TimeUnit.class))).thenReturn(
        Observable.error(new CouchbaseException()));
    assertTrue(writeFunction.putAsync(key, value).isCompletedExceptionally());
  }

  @Test
  public void testPutAsyncJsonObjectValue() throws Exception {
    String key = "key";
    JsonObject value = JsonObject.fromJson("{\"id\": 1}");
    Bucket bucket = mock(Bucket.class);
    AsyncBucket asyncBucket = mock(AsyncBucket.class);
    CouchbaseTableWriteFunction<JsonObject> writeFunction = createAndInit(bucket, asyncBucket);
    when(asyncBucket.upsert(any(Document.class), anyLong(), any(TimeUnit.class))).thenReturn(Observable.just(null));
    assertNull(writeFunction.putAsync(key, value).get());
  }

  @Test
  public void testPutAsyncStringValue() throws Exception {
    String key = "key";
    String value = "value";
    Bucket bucket = mock(Bucket.class);
    AsyncBucket asyncBucket = mock(AsyncBucket.class);
    CouchbaseTableWriteFunction<String> writeFunction =
        createAndInit(String.class, new StringSerde(), bucket, asyncBucket);
    when(asyncBucket.upsert(any(Document.class), anyLong(), any(TimeUnit.class))).thenReturn(Observable.just(null));
    assertNull(writeFunction.putAsync(key, value).get());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDeleteAsyncNullKey() {
    CouchbaseTableWriteFunction<JsonObject> writeFunction = createAndInit();
    writeFunction.deleteAsync(null);
  }

  @Test
  public void testDeleteAsyncException() {
    String key = "throwExceptionKey";
    Bucket bucket = mock(Bucket.class);
    AsyncBucket asyncBucket = mock(AsyncBucket.class);
    CouchbaseTableWriteFunction<JsonObject> writeFunction = createAndInit(bucket, asyncBucket);
    when(asyncBucket.remove(eq(key), anyLong(), any(TimeUnit.class))).thenReturn(
        Observable.error(new CouchbaseException()));
    assertTrue(writeFunction.deleteAsync(key).isCompletedExceptionally());
  }

  @Test
  public void testDeleteAsyncJsonObjectValue() throws Exception {
    String key = "key";
    Bucket bucket = mock(Bucket.class);
    AsyncBucket asyncBucket = mock(AsyncBucket.class);
    CouchbaseTableWriteFunction<JsonObject> writeFunction = createAndInit(bucket, asyncBucket);
    when(asyncBucket.remove(eq(key), anyLong(), any(TimeUnit.class))).thenReturn(Observable.just(null));
    assertNull(writeFunction.deleteAsync(key).get());
  }

  @Test
  public void testDeleteAsyncStringValue() throws Exception {
    String key = "key";
    Bucket bucket = mock(Bucket.class);
    AsyncBucket asyncBucket = mock(AsyncBucket.class);
    CouchbaseTableWriteFunction<String> writeFunction =
        createAndInit(String.class, new StringSerde(), bucket, asyncBucket);
    when(asyncBucket.remove(eq(key), anyLong(), any(TimeUnit.class))).thenReturn(Observable.just(null));
    assertNull(writeFunction.deleteAsync(key).get());
  }

  private CouchbaseTableWriteFunction<JsonObject> createAndInit() {
    return createAndInit(mock(Bucket.class), mock(AsyncBucket.class));
  }

  private CouchbaseTableWriteFunction<JsonObject> createAndInit(Bucket bucket, AsyncBucket asyncBucket) {
    return createAndInit(JsonObject.class, null, bucket, asyncBucket);
  }

  private <V> CouchbaseTableWriteFunction<V> createAndInit(Class<V> valueClass, Serde<V> serde, Bucket bucket,
      AsyncBucket asyncBucket) {
    when(bucket.async()).thenReturn(asyncBucket);
    PowerMockito.stub(PowerMockito.method(CouchbaseBucketRegistry.class, "getBucket", String.class, List.class,
        CouchbaseEnvironmentConfigs.class)).toReturn(bucket);
    CouchbaseTableWriteFunction<V> writeFunction =
        new CouchbaseTableWriteFunction<>(DEFAULT_BUCKET_NAME, valueClass, DEFAULT_CLUSTER_NODE).withSerde(serde);
    writeFunction.init(mock(Context.class), mock(AsyncReadWriteUpdateTable.class));
    return writeFunction;
  }
}
