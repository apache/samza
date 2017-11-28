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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;


/**
 * AWSCredentialsProvider implementation that takes in accessKey and secretKey directly. Requires both accessKey and
 * secretKey to be non-null for it to create a BasicAWSCredentials instance. Otherwise, it creates an AWSCredentials
 * instance with null keys.
 */
public class KinesisAWSCredentialsProvider implements AWSCredentialsProvider {
  private final AWSCredentials creds;
  private static final Logger LOG = LoggerFactory.getLogger(KinesisAWSCredentialsProvider.class.getName());

  public KinesisAWSCredentialsProvider(String accessKey, String secretKey) {
    if (StringUtils.isEmpty(accessKey) || StringUtils.isEmpty(secretKey)) {
      creds = new AWSCredentials() {
        @Override
        public String getAWSAccessKeyId() {
          return null;
        }

        @Override
        public String getAWSSecretKey() {
          return null;
        }
      };
      LOG.info("Could not load credentials from KinesisAWSCredentialsProvider");
    } else {
      creds = new BasicAWSCredentials(accessKey, secretKey);
      LOG.info("Loaded credentials from KinesisAWSCredentialsProvider");
    }
  }

  @Override
  public AWSCredentials getCredentials() {
    return creds;
  }

  @Override
  public void refresh() {
    //no-op
  }
}
