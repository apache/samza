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
import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.context.Context;
import org.apache.samza.operators.functions.ClosableFunction;
import org.apache.samza.operators.functions.InitableFunction;
import org.apache.samza.serializers.Serde;


/**
 * Base class for {@link CouchbaseTableReadFunction} and {@link CouchbaseTableWriteFunction}
 * @param <V> Type of values to read from / write to couchbase
 */
public abstract class BaseCouchbaseTableFunction<V> implements InitableFunction, ClosableFunction, Serializable {

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
  protected CouchbaseEnvironmentConfigs environmentConfigs;

  /**
   * Constructor for BaseCouchbaseTableFunction. This constructor abstracts the shareable logic of the read and write
   * functions. It is not intended to be called directly.
   * @param bucketName Name of the Couchbase bucket
   * @param clusterNodes Some Hosts of the Couchbase cluster. Recommended to provide more than one nodes so that if
   *                     the first node could not be connected, other nodes can be tried.
   * @param valueClass type of values
   */
  public BaseCouchbaseTableFunction(String bucketName, List<String> clusterNodes, Class<V> valueClass) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(bucketName), "Bucket name is not allowed to be null or empty.");
    Preconditions.checkArgument(CollectionUtils.isNotEmpty(clusterNodes),
        "Cluster nodes is not allowed to be null or empty.");
    Preconditions.checkArgument(valueClass != null, "Value class is not allowed to be null.");
    this.bucketName = bucketName;
    this.clusterNodes = ImmutableList.copyOf(clusterNodes);
    this.environmentConfigs = new CouchbaseEnvironmentConfigs();
  }

  /**
   * Helper method to initialize {@link Bucket}.
   */
  @Override
  public void init(Context context) {
    bucket = COUCHBASE_BUCKET_REGISTRY.getBucket(bucketName, clusterNodes, environmentConfigs);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    COUCHBASE_BUCKET_REGISTRY.closeBucket(bucketName, clusterNodes);
  }

  /**
   * Check whether the exception is caused by one of the temporary failure exceptions, which are
   * likely to be retriable.
   */
  public boolean isRetriable(Throwable exception) {
    while (exception != null && !(exception instanceof TemporaryFailureException)
        && !(exception instanceof TemporaryLockFailureException)) {
      exception = exception.getCause();
    }
    return exception != null;
  }

  /**
   * Set the timeout limit on the read / write operations.
   * See <a href="https://docs.couchbase.com/java-sdk/2.7/client-settings.html#timeout-options"/>.
   * @param timeout Timeout duration
   * @return Self
   */
  public <T extends BaseCouchbaseTableFunction<V>> T withTimeout(Duration timeout) {
    this.timeout = timeout;
    return (T) this;
  }

  /**
   * Set the TTL for the data writen to Couchbase.
   * See <a href="https://docs.couchbase.com/java-sdk/2.7/core-operations.html#expiry"/>.
   * @param ttl TTL duration
   * @return Self
   */
  public <T extends BaseCouchbaseTableFunction<V>> T withTtl(Duration ttl) {
    this.ttl = ttl;
    return (T) this;
  }

  /**
   * Serde is used to serialize and deserialize values to/from byte array. If value type is not
   * {@link com.couchbase.client.java.document.json.JsonObject}, a Serde must be provided.
   * @param valueSerde value serde
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
   * @return Self
   */
  public <T extends BaseCouchbaseTableFunction<V>> T withUsernameAndPassword(String username, String password) {
    if (environmentConfigs.sslEnabled) {
      throw new IllegalArgumentException(
          "Role-Based Access Control and Certificate-Based Authentication cannot be used together.");
    }
    environmentConfigs.username = username;
    environmentConfigs.password = password;
    return (T) this;
  }

  /**
   * Enable certificate-based authentication. If ssl is enabled sslKeystore or sslTrustStore should also be provided
   * accordingly.
   * @param sslEnabled allows to enable certificate-based authentication
   * @param certAuthEnabled allows to enable X.509 client certificate authentication
   * @return
   */
  public <T extends BaseCouchbaseTableFunction<V>> T withSslEnabledAndCertAuthEnabled(boolean sslEnabled,
      boolean certAuthEnabled) {
    if (environmentConfigs.username != null && sslEnabled) {
      throw new IllegalArgumentException(
          "Role-Based Access Control and Certificate-Based Authentication cannot be used together.");
    }
    if (certAuthEnabled && !sslEnabled) {
      throw new IllegalStateException(
          "Client Certificate Authentication enabled, but SSL is not - " + "please configure encryption properly.");
    }
    environmentConfigs.sslEnabled = sslEnabled;
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
   */
  public <T extends BaseCouchbaseTableFunction<V>> T withSslKeystoreFileAndPassword(String sslKeystoreFile,
      String sslKeystorePassword) {
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
   */
  public <T extends BaseCouchbaseTableFunction<V>> T withSslTruststoreFileAndPassword(String sslTruststoreFile,
      String sslTruststorePassword) {
    environmentConfigs.sslTruststoreFile = sslTruststoreFile;
    environmentConfigs.sslTruststorePassword = sslTruststorePassword;
    return (T) this;
  }

  /**
   * If carrier publication bootstrap is enabled and not SSL, sets the port to use.
   * Default value see
   * <a href="https://docs.couchbase.com/server/6.0/learn/clusters-and-availability/connectivity.html#section-client-2-cluster-comm"/>.
   */
  public <T extends BaseCouchbaseTableFunction<V>> T withBootstrapCarrierDirectPort(int bootstrapCarrierDirectPort) {
    environmentConfigs.bootstrapCarrierDirectPort = bootstrapCarrierDirectPort;
    return (T) this;
  }

  /**
   * If carrier publication bootstrap and SSL are enabled, sets the port to use.
   * Default value see
   * <a href="https://docs.couchbase.com/server/6.0/learn/clusters-and-availability/connectivity.html#section-client-2-cluster-comm"/>.
   */
  public <T extends BaseCouchbaseTableFunction<V>> T withBootstrapCarrierSslPort(int bootstrapCarrierSslPort) {
    environmentConfigs.bootstrapCarrierSslPort = bootstrapCarrierSslPort;
    return (T) this;
  }

  /**
   * If Http bootstrap is enabled and not SSL, sets the port to use.
   * Default value see
   * <a href="https://docs.couchbase.com/server/6.0/learn/clusters-and-availability/connectivity.html#section-client-2-cluster-comm"/>.
   */
  public <T extends BaseCouchbaseTableFunction<V>> T withBootstrapHttpDirectPort(int bootstrapHttpDirectPort) {
    environmentConfigs.bootstrapHttpDirectPort = bootstrapHttpDirectPort;
    return (T) this;
  }

  /**
   * If Http bootstrap and SSL are enabled, sets the port to use.
   * Default value see
   * <a href="https://docs.couchbase.com/server/6.0/learn/clusters-and-availability/connectivity.html#section-client-2-cluster-comm"/>.
   */
  public <T extends BaseCouchbaseTableFunction<V>> T withBootstrapHttpSslPort(int bootstrapHttpSslPort) {
    environmentConfigs.bootstrapHttpSslPort = bootstrapHttpSslPort;
    return (T) this;
  }
}
