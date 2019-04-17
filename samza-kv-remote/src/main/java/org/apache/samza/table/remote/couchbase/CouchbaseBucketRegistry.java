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
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.auth.CertAuthenticator;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The CouchbaseBucketRegistry is intended to reuse the same {@link Cluster} instance given same clusterNodes and reuse
 * the same {@link Bucket} given same bucketName. Instantiating a Bucket is expensive. Different tasks within a
 * container that is using the same bucket should use this registry to avoid creating multiple Buckets.
 */
public class CouchbaseBucketRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseBucketRegistry.class);
  private final Map<String, Bucket> openedBuckets;
  private final Map<String, Cluster> openedClusters;
  private final Map<String, Integer> bucketUsageCounts;
  private final Map<String, Integer> clusterUsageCounts;

  /**
   * Constructor of the CouchbaseTableRegistry.
   */
  public CouchbaseBucketRegistry() {
    openedBuckets = new HashMap<>();
    openedClusters = new HashMap<>();
    bucketUsageCounts = new HashMap<>();
    clusterUsageCounts = new HashMap<>();
  }

  /**
   * A synchronized method to open a bucket given the bucket name and cluster nodes. Once a bucket has been opened,
   * the reference of the bucket will be stored in the registry, and the same reference will be returned given same
   * bucket name. Each time this method is called, the counter of usage of the bucket is increased by one.
   * @param bucketName name of the bucket to be opened
   * @param clusterNodes list of cluster nodes
   * @param configs couchbase environment configs
   * @return A Bucket instance associated with the bucket name and cluster nodes
   */
  public synchronized Bucket getBucket(String bucketName, List<String> clusterNodes,
      CouchbaseEnvironmentConfigs configs) {
    String bucketId = getBucketId(bucketName, clusterNodes);
    String clusterId = getClusterId(clusterNodes);
    if (!openedClusters.containsKey(clusterId)) {
      openedClusters.put(clusterId, openCluster(clusterNodes, configs));
    }
    if (!openedBuckets.containsKey(bucketId)) {
      openedBuckets.put(bucketId, openBucket(bucketName, openedClusters.get(clusterId)));
    }
    bucketUsageCounts.put(bucketId, bucketUsageCounts.getOrDefault(bucketId, 0) + 1);
    clusterUsageCounts.put(clusterId, clusterUsageCounts.getOrDefault(clusterId, 0) + 1);
    return openedBuckets.get(bucketId);
  }

  /**
   * A synchronized method to close a bucket given the bucket name. Each time this method is called, the counter of
   * usage of the bucket is decreased by one. Only when the counter becomes zero will the bucket be actually closed.
   * Same for cluster, only when all buckets within a cluster have been closed will the cluster be closed.
   * @param bucketName name of the bucket to be opened
   * @param clusterNodes list of cluster nodes
   * @return true if bucket is closed successfully, otherwise false.
   */
  public synchronized boolean closeBucket(String bucketName, List<String> clusterNodes) {
    String bucketId = getBucketId(bucketName, clusterNodes);
    String clusterId = getClusterId(clusterNodes);
    if (!openedBuckets.containsKey(bucketId) || !openedClusters.containsKey(clusterId)) {
      return false;
    }
    bucketUsageCounts.put(bucketId, bucketUsageCounts.get(bucketId) - 1);
    clusterUsageCounts.put(clusterId, clusterUsageCounts.get(clusterId) - 1);
    Boolean bucketClosed = true;
    Boolean clusterClosed = true;
    if (bucketUsageCounts.get(bucketId) == 0) {
      bucketClosed = openedBuckets.get(bucketId).close();
      openedBuckets.remove(bucketId);
      bucketUsageCounts.remove(bucketId);
      if (clusterUsageCounts.get(clusterId) == 0) {
        clusterClosed = openedClusters.get(clusterId).disconnect();
        openedClusters.remove(clusterId);
        clusterUsageCounts.remove(clusterId);
      }
    }
    return bucketClosed && clusterClosed;
  }

  /**
   * Helper method to open a cluster given cluster nodes and environment configurations.
   */
  private Cluster openCluster(List<String> clusterNodes, CouchbaseEnvironmentConfigs configs) {
    DefaultCouchbaseEnvironment.Builder envBuilder = new DefaultCouchbaseEnvironment.Builder();
    if (configs.sslEnabled != null) {
      envBuilder.sslEnabled(configs.sslEnabled);
    }
    if (configs.certAuthEnabled != null) {
      envBuilder.certAuthEnabled(configs.certAuthEnabled);
    }
    if (configs.sslKeystoreFile != null) {
      envBuilder.sslKeystoreFile(configs.sslKeystoreFile);
    }
    if (configs.sslKeystorePassword != null) {
      envBuilder.sslKeystorePassword(configs.sslKeystorePassword);
    }
    if (configs.sslTruststoreFile != null) {
      envBuilder.sslTruststoreFile(configs.sslTruststoreFile);
    }
    if (configs.sslTruststorePassword != null) {
      envBuilder.sslTruststorePassword(configs.sslTruststorePassword);
    }
    if (configs.bootstrapCarrierDirectPort != null) {
      envBuilder.bootstrapCarrierDirectPort(configs.bootstrapCarrierDirectPort);
    }
    if (configs.bootstrapCarrierSslPort != null) {
      envBuilder.bootstrapCarrierSslPort(configs.bootstrapCarrierSslPort);
    }
    if (configs.bootstrapHttpDirectPort != null) {
      envBuilder.bootstrapHttpDirectPort(configs.bootstrapHttpDirectPort);
    }
    if (configs.bootstrapHttpSslPort != null) {
      envBuilder.bootstrapHttpSslPort(configs.bootstrapHttpSslPort);
    }
    CouchbaseEnvironment env = envBuilder.build();
    Cluster cluster = CouchbaseCluster.create(env, clusterNodes);
    if (configs.sslEnabled != null && configs.sslEnabled) {
      cluster.authenticate(CertAuthenticator.INSTANCE);
    } else if (configs.username != null) {
      cluster.authenticate(configs.username, configs.password);
    } else {
      LOGGER.warn("No authentication is enabled for cluster: {}. This is not recommended except for test cases.",
          clusterNodes);
    }
    return cluster;
  }

  /**
   * Helper method to open a bucket with given bucket name and cluster
   */
  private Bucket openBucket(String bucketName, Cluster cluster) {
    return cluster.openBucket(bucketName);
  }

  /**
   * Generate a unique bucketId given bucket name and cluster nodes.
   */
  private String getBucketId(String bucketName, List<String> clusterNodes) {
    return getClusterId(clusterNodes) + "-" + bucketName;
  }

  /**
   * Generate a unique clusterId given cluster nodes.
   */
  private String getClusterId(List<String> clusterNodes) {
    return clusterNodes.toString();
  }

  static class CouchbaseEnvironmentConfigs implements Serializable {

    Boolean sslEnabled;
    Boolean certAuthEnabled;
    String sslKeystoreFile;
    String sslKeystorePassword;
    String sslTruststoreFile;
    String sslTruststorePassword;
    Integer bootstrapCarrierDirectPort;
    Integer bootstrapCarrierSslPort;
    Integer bootstrapHttpDirectPort;
    Integer bootstrapHttpSslPort;
    String username;
    String password;
  }
}
