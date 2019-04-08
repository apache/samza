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
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.powermock.api.mockito.PowerMockito.*;

/**
 * This class performs unit tests for CouchbaseBucketRegistry
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(CouchbaseCluster.class)
public class TestCouchbaseBucketRegistry {

  /**
   * This unit test uses CouchbaseBucketRegistry to register two mocked buckets. It tests:
   * 1. Calling registry.getBucket with same bucketName and clusterNodes should return same Bucket instance
   * 2. Calling registry.getBucket with different bucketNames should return different Bucket instances
   */
  @Test
  public void testOpenBuckets() {
    String bucketName1 = "bucket1";
    String bucketName2 = "bucket2";
    List<String> clusterNodes = new ArrayList<>(Arrays.asList("cluster"));
    CouchbaseEnvironmentConfigs configs = new CouchbaseEnvironmentConfigs();
    CouchbaseCluster cluster = mock(CouchbaseCluster.class);
    when(cluster.openBucket(bucketName1)).thenReturn(mock(Bucket.class));
    when(cluster.openBucket(bucketName2)).thenReturn(mock(Bucket.class));
    mockStatic(CouchbaseCluster.class);
    when(CouchbaseCluster.create(any(CouchbaseEnvironment.class), any(List.class))).thenReturn(cluster);
    CouchbaseBucketRegistry registry = new CouchbaseBucketRegistry();
    Bucket bucket1 = registry.getBucket(bucketName1, clusterNodes, configs);
    Bucket bucket1_copy = registry.getBucket(bucketName1, clusterNodes, configs);
    Bucket bucket2 = registry.getBucket(bucketName2, clusterNodes, configs);
    assertEquals(bucket1, bucket1_copy);
    assertNotEquals(bucket1, bucket2);
  }

  /**
   * This unit test uses CouchbaseBucketRegistry to register two mocked buckets with same name but in different clusters.
   * Calling registry.getBucket with same bucketName but different clusterNodes should return different Bucket instances.
   */
  @Test
  public void testOpenSameBucketNameFromDifferentClusters() {
    String bucketName = "bucket";
    List<String> clusterNodes1 = new ArrayList<>(Arrays.asList("cluster1"));
    List<String> clusterNodes2 = new ArrayList<>(Arrays.asList("cluster2"));
    CouchbaseEnvironmentConfigs configs = new CouchbaseEnvironmentConfigs();
    CouchbaseCluster cluster1 = mock(CouchbaseCluster.class);
    CouchbaseCluster cluster2 = mock(CouchbaseCluster.class);
    when(cluster1.openBucket(bucketName)).thenReturn(mock(Bucket.class));
    when(cluster2.openBucket(bucketName)).thenReturn(mock(Bucket.class));
    mockStatic(CouchbaseCluster.class);
    when(CouchbaseCluster.create(any(CouchbaseEnvironment.class), eq(clusterNodes1))).thenReturn(cluster1);
    when(CouchbaseCluster.create(any(CouchbaseEnvironment.class), eq(clusterNodes2))).thenReturn(cluster2);
    CouchbaseBucketRegistry registry = new CouchbaseBucketRegistry();
    Bucket bucketInCluster1 = registry.getBucket(bucketName, clusterNodes1, configs);
    Bucket bucketInCluster2 = registry.getBucket(bucketName, clusterNodes2, configs);
    assertNotEquals(bucketInCluster1, bucketInCluster2);
  }

  /**
   * This unit test simulates 10 tasks using the same bucket. Each task will call registry.getBucket once. Then
   * each task will also call registry.closeBucket once. After that, registry.closeBucket should return false if we
   * close the bucket one more time. And the bucket should have already been closed.
   */
  @Test
  public void testCloseBucket() {
    String bucketName = "bucket";
    List<String> clusterNodes = new ArrayList<>(Arrays.asList("cluster"));
    CouchbaseEnvironmentConfigs configs = new CouchbaseEnvironmentConfigs();
    CouchbaseCluster cluster = mock(CouchbaseCluster.class);
    Bucket bucket = mock(Bucket.class);
    when(bucket.close()).thenReturn(true).thenReturn(false);
    when(cluster.openBucket(bucketName)).thenReturn(bucket);
    when(cluster.disconnect()).thenReturn(true).thenReturn(false);
    mockStatic(CouchbaseCluster.class);
    when(CouchbaseCluster.create(any(CouchbaseEnvironment.class), eq(clusterNodes))).thenReturn(cluster);
    CouchbaseBucketRegistry registry = new CouchbaseBucketRegistry();
    int numOfThreads = 10;
    for (int i = 0; i < numOfThreads; i++) {
      registry.getBucket(bucketName, clusterNodes, configs);
    }
    for (int i = 0; i < numOfThreads; i++) {
      assertTrue(registry.closeBucket(bucketName, clusterNodes));
    }
    // Close one more time. Should return false.
    assertFalse(registry.closeBucket(bucketName, clusterNodes));
    // Bucket should has been closed
    assertFalse(bucket.close());
  }

  /**
   * This unit test simulates closing two buckets within one cluster. The cluster should only be disconnected when all
   * buckets has been closed.
   */
  @Test
  public void testCloseTwoBucketsInSameCluster() {
    String bucketName1 = "bucket1";
    String bucketName2 = "bucket2";
    List<String> clusterNodes = new ArrayList<>(Arrays.asList("cluster"));
    CouchbaseEnvironmentConfigs configs = new CouchbaseEnvironmentConfigs();
    CouchbaseCluster cluster = mock(CouchbaseCluster.class);
    Bucket bucket1 = mock(Bucket.class);
    Bucket bucket2 = mock(Bucket.class);
    when(bucket1.close()).thenReturn(true).thenReturn(false);
    when(bucket2.close()).thenReturn(true).thenReturn(false);
    when(cluster.openBucket(bucketName1)).thenReturn(bucket1);
    when(cluster.openBucket(bucketName2)).thenReturn(bucket2);
    when(cluster.disconnect()).thenReturn(true).thenReturn(false);
    mockStatic(CouchbaseCluster.class);
    when(CouchbaseCluster.create(any(CouchbaseEnvironment.class), eq(clusterNodes))).thenReturn(cluster);
    CouchbaseBucketRegistry registry = new CouchbaseBucketRegistry();
    registry.getBucket(bucketName1, clusterNodes, configs);
    registry.getBucket(bucketName2, clusterNodes, configs);
    assertTrue(registry.closeBucket(bucketName1, clusterNodes));
    assertTrue(registry.closeBucket(bucketName2, clusterNodes));
    // Cluster should have been disconnected. Should return false.
    assertFalse(cluster.disconnect());
    // Buckets should have been closed. Should return false.
    assertFalse(cluster.disconnect());
  }
}
