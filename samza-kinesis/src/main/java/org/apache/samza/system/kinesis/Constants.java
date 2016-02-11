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

/**
 * Contains all the constants needed by the Kinesis' consumers/producers.
 */
public class Constants {
    // Configurations coming from the kinesis-consumer-stream.properties
    /**
     * AWS credentials path
     */
    public static final String CONFIG_PATH_PARAM = "config-file-path";

    /**
     * AWS stream name
     */
    public static final String STREAM_NAME_PARAM = "stream-name";

    /**
     * Application that will consume such stream
     */
    public static final String APP_NAME_PARAM = "job.name";

    /**
     * Position in the AWS stream from where we start consuming
     */
    public static final String STREAM_POSITION_PARAM = "position-in-stream";
    public static final String SEQUENCE_NUMBER_PARAM = "sequence-number";
    public static final String SEQUENCE_NUMBER_SUFFIX = "SEQUENCE_NUMBER";
    public static final String SHARDID_PREFFIX = "shardId-%012d";
    public static final String MAX_REQUEST_RECORDS_PARAM = "max-request-records";

    /**
     * Aws region parameter
     */
    public static final String AWS_REGION_PARAM = "aws-region";

    // TODO this still need to be set up
    /**
     * Maximum number of records consumed by record processor.
     */
    public static final String MAX_RECORDS_PARAM = "max-records";

    /**
     * Default failures to be tolerated when reading from a kinesis stream
     */
    public static final String FAILURES_TOLERATED_PARAM = "failures-tolerated";

    /**
     * Environment from where the application is being executed
     */
    public static final String ENVIRONMENT_PARAM = "environment";
    /**
     * Parameters for auto-creating a stream if it doesn't exist
     */
    public static final String AUTO_CREATE_STREAM = "auto-create-stream";
    public static final String NUMBER_SHARD = "number-shards";
    public static final int DEFAULT_NUM_SHARDS = 1;

    // Default configuration values values
    /**
     * Default failures tolerated when consuming Kinesis streams.
     */
    public static final int DEFAULT_FAILURES_TOLERATED = -1;

    /**
     * Default number of checkpoint retires.
     */
    public static final int DEFAULT_NUM_RETRIES = 10;

    /**
     * Backoff time when trying to checkpoint
     */
    public static final long DEFAULT_BACKOFF_TIME_IN_MILLIS = 100L;

    /**
     * Default max number of recors to be consumed
     */
    public static final int DEFAULT_MAX_RECORDS = -1;
}
