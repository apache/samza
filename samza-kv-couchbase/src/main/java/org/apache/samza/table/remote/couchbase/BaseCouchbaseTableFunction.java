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

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.couchbase.client.java.error.TemporaryLockFailureException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.time.Duration;
import java.util.List;

import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.context.Context;
import org.apache.samza.serializers.Serde;
import org.apache.samza.table.AsyncReadWriteUpdateTable;
import org.apache.samza.table.remote.BaseTableFunction;


/**
 * Base class for {@link CouchbaseTableReadFunction} and {@link CouchbaseTableWriteFunction}
 * @param <V> Type of values to read from / write to couchbase
 */
public abstract class BaseCouchbaseTableFunction<V> extends BaseTableFunction {

  // Clients
  private final static CouchbaseBucketRegistry COUCHBASE_BUCKET_REGISTRY = new CouchbaseBucketRegistry();
  protected transient Bucket bucket;

  // Function Settings
  protected Serde<V> valueSerde = null;
  protected Duration timeout = Duration.ZERO; // default value 0 means no timeout
  protected Duration ttl = Duration.ZERO; // default value 0 means no ttl, data will be stored forever

  // Cluster Settings
  protected final List<String> clusterNodes;
  protected final String bucketName;

  // Environment Settings
  protected final CouchbaseEnvironmentConfigs environmentConfigs;

  /**
   * Constructor for BaseCouchbaseTableFunction. This constructor abstracts the shareable logic of the read and write
   * functions. It is not intended to be called directly.
   * @param bucketName Name of the Couchbase bucket
   * @param valueClass type of values
   * @param clusterNodes Some Hosts of the Couchbase cluster. Recommended to provide more than one nodes so that if
   *                     the first node could not be connected, other nodes can be tried.
   */
  public BaseCouchbaseTableFunction(String bucketName, Class<V> valueClass, String... clusterNodes) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(bucketName), "Bucket name is not allowed to be null or empty.");
    Preconditions.checkArgument(valueClass != null, "Value class is not allowed to be null.");
    Preconditions.checkArgument(ArrayUtils.isNotEmpty(clusterNodes),
        "Cluster nodes is not allowed to be null or empty.");
    this.bucketName = bucketName;
    this.clusterNodes = ImmutableList.copyOf(clusterNodes);
    environmentConfigs = new CouchbaseEnvironmentConfigs();
  }

  @Override
  public void init(Context context, AsyncReadWriteUpdateTable table) {
    super.init(context, table);
    bucket = COUCHBASE_BUCKET_REGISTRY.getBucket(bucketName, clusterNodes, environmentConfigs);
  }

  @Override
  public void close() {
    COUCHBASE_BUCKET_REGISTRY.closeBucket(bucketName, clusterNodes);
  }

  /**
   * Check whether the exception is caused by one of the temporary failure exceptions, which are
   * likely to be retriable.
   * @param exception exception thrown by the table provider
   * @return true if we should retry, otherwise false
   */
  public boolean isRetriable(Throwable exception) {
    while (exception != null
        && !(exception instanceof TemporaryFailureException)
        && !(exception instanceof TemporaryLockFailureException)
        && !(exception instanceof TimeoutException)) {
      exception = exception.getCause();
    }
    return exception != null;
  }

  /**
   * Set the timeout limit on the read / write operations. Default value is Duration.ZERO, which means no timeout.
   * See <a href="https://docs.couchbase.com/java-sdk/2.7/client-settings.html#timeout-options"></a>.
   * @param timeout Timeout duration
   * @param <T> type of this instance
   * @return Self
   */
  public <T extends BaseCouchbaseTableFunction<V>> T withTimeout(Duration timeout) {
    Preconditions.checkArgument(timeout != null && !timeout.isNegative(), "Timeout should not be null or negative");
    this.timeout = timeout;
    return (T) this;
  }

  /**
   * Set the TTL for the data writen to Couchbase. Default value Duration.ZERO means no TTL, data will be stored forever.
   * See <a href="https://docs.couchbase.com/java-sdk/2.7/core-operations.html#expiry"></a>.
   * @param ttl TTL duration
   * @param <T> type of this instance
   * @return Self
   */
  public <T extends BaseCouchbaseTableFunction<V>> T withTtl(Duration ttl) {
    Preconditions.checkArgument(ttl != null && !ttl.isNegative(), "TTL should not be null or negative");
    this.ttl = ttl;
    return (T) this;
  }

  /**
   * Serde is used to serialize and deserialize values to/from byte array. If value type is not
   * {@link com.couchbase.client.java.document.json.JsonObject}, a Serde must be provided.
   * @param valueSerde value serde
   * @param <T> type of this instance
   * @return Self
   */
  public <T extends BaseCouchbaseTableFunction<V>> T withSerde(Serde<V> valueSerde) {
    this.valueSerde = valueSerde;
    return (T) this;
  }

  /**
   * Enable role-based authentication with username and password. Note that role-based and certificate-based
   * authentications can not be used together.
   * @param username username
   * @param password password
   * @param <T> type of this instance
   * @return Self
   */
  public <T extends BaseCouchbaseTableFunction<V>> T withUsernameAndPassword(String username, String password) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(username), "username should not be null or empty.");
    if (environmentConfigs.sslEnabled != null && environmentConfigs.sslEnabled) {
      throw new IllegalArgumentException(
          "Role-Based Access Control and Certificate-Based Authentication cannot be used together.");
    }
    environmentConfigs.username = username;
    environmentConfigs.password = password;
    return (T) this;
  }

  /**
   * Enable certificate-based authentication and set sslEnabled to be true. If certAuthEnabled is false, only server
   * side certificate checking is enabled. If certAuthEnabled is true, both client and server certificate checking are
   * enabled.
   * SslKeystore or sslTrustStore should also be provided accordingly.
   * @param certAuthEnabled allows to enable X.509 client certificate authentication
   * @param <T> type of this instance
   * @return Self
   */
  public <T extends BaseCouchbaseTableFunction<V>> T withSslEnabledAndCertAuthEnabled(boolean certAuthEnabled) {
    if (environmentConfigs.username != null) {
      throw new IllegalArgumentException(
          "Role-Based Access Control and Certificate-Based Authentication cannot be used together.");
    }
    environmentConfigs.sslEnabled = true;
    environmentConfigs.certAuthEnabled = certAuthEnabled;
    return (T) this;
  }

  /**
   * Defines the location and password of the SSL Keystore file (default value null).
   *
   * If this method is used without also specifying
   * {@link #withSslTruststoreFileAndPassword} this keystore will be used to initialize
   * both the key factory as well as the trust factory with java SSL. This
   * needs to be the case for backwards compatibility, but if you do not need
   * X.509 client cert authentication you might as well just use {@link #withSslTruststoreFileAndPassword}
   * alone.
   * @param sslKeystoreFile  path of ssl keystore file
   * @param sslKeystorePassword password of ssl keystore
   * @param <T> type of this instance
   * @return Self
   */
  public <T extends BaseCouchbaseTableFunction<V>> T withSslKeystoreFileAndPassword(String sslKeystoreFile,
      String sslKeystorePassword) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(sslKeystoreFile), "Null or empty sslKeystoreFile");
    Preconditions.checkArgument(StringUtils.isNotEmpty(sslKeystorePassword), "Null or empty sslKeystorePassword");
    environmentConfigs.sslKeystoreFile = sslKeystoreFile;
    environmentConfigs.sslKeystorePassword = sslKeystorePassword;
    return (T) this;
  }

  /**
   * Defines the location and password of the SSL TrustStore keystore file (default value null).
   *
   * If this method is used without also specifying
   * {@link #withSslKeystoreFileAndPassword} this keystore will be used to initialize
   * both the key factory as well as the trust factory with java SSL. Prefer
   * this method over the {@link #withSslKeystoreFileAndPassword} if you do not need
   * X.509 client auth and just need server side certificate checking.
   * @param sslTruststoreFile path of truststore file
   * @param sslTruststorePassword password of truststore
   * @param <T> type of this instance
   * @return Self
   */
  public <T extends BaseCouchbaseTableFunction<V>> T withSslTruststoreFileAndPassword(String sslTruststoreFile,
      String sslTruststorePassword) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(sslTruststoreFile), "Null or empty sslTruststoreFile");
    Preconditions.checkArgument(StringUtils.isNotEmpty(sslTruststorePassword), "Null or empty sslTruststorePassword");
    environmentConfigs.sslTruststoreFile = sslTruststoreFile;
    environmentConfigs.sslTruststorePassword = sslTruststorePassword;
    return (T) this;
  }

  /**
   * If carrier publication bootstrap is enabled and not SSL, sets the port to use.
   * Default value see
   * <a href="https://docs.couchbase.com/server/6.0/learn/clusters-and-availability/connectivity.html#section-client-2-cluster-comm"></a>.
   * @param bootstrapCarrierDirectPort bootstrap carrier direct port
   * @param <T> type of this instance
   * @return Self
   */
  public <T extends BaseCouchbaseTableFunction<V>> T withBootstrapCarrierDirectPort(int bootstrapCarrierDirectPort) {
    environmentConfigs.bootstrapCarrierDirectPort = bootstrapCarrierDirectPort;
    return (T) this;
  }

  /**
   * If carrier publication bootstrap and SSL are enabled, sets the port to use.
   * Default value see
   * <a href="https://docs.couchbase.com/server/6.0/learn/clusters-and-availability/connectivity.html#section-client-2-cluster-comm"></a>.
   * @param bootstrapCarrierSslPort bootstrap carrier ssl port
   * @param <T> type of this instance
   * @return Self
   */
  public <T extends BaseCouchbaseTableFunction<V>> T withBootstrapCarrierSslPort(int bootstrapCarrierSslPort) {
    environmentConfigs.bootstrapCarrierSslPort = bootstrapCarrierSslPort;
    return (T) this;
  }

  /**
   * If Http bootstrap is enabled and not SSL, sets the port to use.
   * Default value see
   * <a href="https://docs.couchbase.com/server/6.0/learn/clusters-and-availability/connectivity.html#section-client-2-cluster-comm"></a>.
   * @param bootstrapHttpDirectPort bootstrap http direct port
   * @param <T> type of this instance
   * @return Self
   */
  public <T extends BaseCouchbaseTableFunction<V>> T withBootstrapHttpDirectPort(int bootstrapHttpDirectPort) {
    environmentConfigs.bootstrapHttpDirectPort = bootstrapHttpDirectPort;
    return (T) this;
  }

  /**
   * If Http bootstrap and SSL are enabled, sets the port to use.
   * Default value see
   * <a href="https://docs.couchbase.com/server/6.0/learn/clusters-and-availability/connectivity.html#section-client-2-cluster-comm"></a>.
   * @param bootstrapHttpSslPort bootstrap http ssl port
   * @param <T> type of this instance
   * @return Self
   */
  public <T extends BaseCouchbaseTableFunction<V>> T withBootstrapHttpSslPort(int bootstrapHttpSslPort) {
    environmentConfigs.bootstrapHttpSslPort = bootstrapHttpSslPort;
    return (T) this;
  }
}
