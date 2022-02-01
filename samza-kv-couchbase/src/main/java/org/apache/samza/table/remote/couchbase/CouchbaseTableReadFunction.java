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

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.java.document.BinaryDocument;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

import com.google.common.base.Preconditions;

import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;

import org.apache.samza.SamzaException;
import org.apache.samza.context.Context;
import org.apache.samza.table.AsyncReadWriteUpdateTable;
import org.apache.samza.table.remote.TableReadFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Single;
import rx.SingleSubscriber;


/**
 * TableReadFunction implementation for reading from Couchbase. The value type can be either {@link JsonObject} or
 * any other Object. If the value type is JsonObject, data in JSON format will be read from Couchbase directly as
 * JsonObject. Otherwise, a {@link org.apache.samza.serializers.Serde} needs to be provided to serialize and
 * deserialize the value object.
 * @param <V> Type of values to read from Couchbase
 */
public class CouchbaseTableReadFunction<V> extends BaseCouchbaseTableFunction<V>
    implements TableReadFunction<String, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseTableReadFunction.class);

  protected final Class<? extends Document<?>> documentType;

  /**
   * Construct an instance of {@link CouchbaseTableReadFunction}.
   * @param bucketName Name of the couchbase bucket
   * @param clusterNodes Some Hosts of the Couchbase cluster. Recommended to provide more than one nodes so that if
   *                     the first node could not be connected, other nodes can be tried.
   * @param valueClass Type of values
   */
  public CouchbaseTableReadFunction(String bucketName, Class<V> valueClass, String... clusterNodes) {
    super(bucketName, valueClass, clusterNodes);
    documentType = JsonObject.class.isAssignableFrom(valueClass) ? JsonDocument.class : BinaryDocument.class;
  }

  @Override
  public void init(Context context, AsyncReadWriteUpdateTable table) {
    super.init(context, table);
    LOGGER.info("Read function for bucket {} initialized successfully", bucketName);
  }

  @Override
  public CompletableFuture<V> getAsync(String key) {
    Preconditions.checkArgument(StringUtils.isNotBlank(key), "key must not be null, empty or blank");
    CompletableFuture<V> future = new CompletableFuture<>();
    Single<? extends Document<?>> singleObservable =
        bucket.async().get(key, documentType, timeout.toMillis(), TimeUnit.MILLISECONDS).toSingle();
    singleObservable.subscribe(new SingleSubscriber<Document<?>>() {
      @Override
      public void onSuccess(Document<?> document) {
        if (document != null) {
          if (document instanceof BinaryDocument) {
            handleGetAsyncBinaryDocument((BinaryDocument) document, future, key);
          } else {
            // V is of type JsonObject
            future.complete((V) document.content());
          }
        } else {
          // The Couchbase async client should not return null
          future.completeExceptionally(new SamzaException(String.format("Got unexpected null value from key %s", key)));
        }
      }

      @Override
      public void onError(Throwable throwable) {
        if (throwable instanceof NoSuchElementException) {
          // There is no element returned by the observable, meaning the key doesn't exist.
          future.complete(null);
        } else {
          future.completeExceptionally(new SamzaException(String.format("Failed to get key %s", key), throwable));
        }
      }
    });
    return future;
  }

  /*
   * Helper method to read bytes from binaryDocument and release the buffer.
   */
  protected void handleGetAsyncBinaryDocument(BinaryDocument binaryDocument, CompletableFuture<V> future, String key) {
    ByteBuf buffer = binaryDocument.content();
    try {
      byte[] bytes;
      if (buffer.hasArray() && buffer.arrayOffset() == 0 && buffer.readableBytes() == buffer.array().length) {
        bytes = buffer.array();
      } else {
        bytes = new byte[buffer.readableBytes()];
        buffer.readBytes(bytes);
      }
      future.complete(valueSerde.fromBytes(bytes));
    } catch (Exception e) {
      future.completeExceptionally(
          new SamzaException(String.format("Failed to deserialize value of key %s with given serde", key), e));
    } finally {
      ReferenceCountUtil.release(buffer);
    }
  }
}
