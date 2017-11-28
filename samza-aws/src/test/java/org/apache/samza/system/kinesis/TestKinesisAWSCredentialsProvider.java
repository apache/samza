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

package org.apache.samza.system.kinesis;

import org.junit.Test;

import static org.junit.Assert.*;


public class TestKinesisAWSCredentialsProvider {

  @Test
  public void testCredentialsProviderWithNonNullKeys() {
    String accessKey = "accessKey";
    String secretKey = "secretKey";
    KinesisAWSCredentialsProvider credProvider = new KinesisAWSCredentialsProvider(accessKey, secretKey);
    assertEquals(credProvider.getCredentials().getAWSAccessKeyId(), accessKey);
    assertEquals(credProvider.getCredentials().getAWSSecretKey(), secretKey);
  }

  @Test
  public void testCredentialsProviderWithNullAccessKey() {
    String secretKey = "secretKey";
    KinesisAWSCredentialsProvider credProvider = new KinesisAWSCredentialsProvider(null, secretKey);
    assertNull(credProvider.getCredentials().getAWSAccessKeyId());
    assertNull(credProvider.getCredentials().getAWSSecretKey());
  }

  @Test
  public void testCredentialsProviderWithNullSecretKey() {
    String accessKey = "accessKey";
    KinesisAWSCredentialsProvider credProvider = new KinesisAWSCredentialsProvider(accessKey, null);
    assertNull(credProvider.getCredentials().getAWSAccessKeyId());
    assertNull(credProvider.getCredentials().getAWSSecretKey());
  }

  @Test
  public void testCredentialsProviderWithNullKeys() {
    KinesisAWSCredentialsProvider credProvider = new KinesisAWSCredentialsProvider(null, null);
    assertNull(credProvider.getCredentials().getAWSAccessKeyId());
    assertNull(credProvider.getCredentials().getAWSSecretKey());
  }
}
