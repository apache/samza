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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Class for all Kinesis stream managing functions
 */
public class KinesisUtils {
    /**
     * Logger for the KinesisSystemConsumer
     */
    private static final Log LOG = LogFactory.getLog(KinesisUtils.class);
    /**
     * Gets the description of a stream
     * @param streamName stream name
     * @param credentials AmazonWebServices credentials
     * @param region AmazonWebServices region
     * @return StreamDescription
     */
    public static StreamDescription getDescriptionStream(String streamName, String credentials, String region) {
        AmazonKinesisClient kClient = getKinesisClient(credentials, region);

        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(streamName);
        StreamDescription streamDescription = null;
        try {
            streamDescription = kClient.describeStream(describeStreamRequest).getStreamDescription();
            LOG.debug("Stream " + streamName + " has a status of " + streamDescription.getStreamStatus());
            LOG.debug("Stream " + streamName + " has " + streamDescription.getShards().size() + " shards.");

            if ("DELETING".equals(streamDescription.getStreamStatus())) {
                LOG.error("Stream is being deleted.");
            }

            if (!"ACTIVE".equals(streamDescription.getStreamStatus())) {
                LOG.warn("Wait for the stream to become active if it is not yet ACTIVE.");
                waitForStreamToBecomeAvailable(streamName, kClient);
            }
        } catch (ResourceNotFoundException ex) {
            LOG.error("Stream " + streamName + " does not exist.");
            ex.printStackTrace();
        } catch (InterruptedException e) {
            LOG.error("Something went wrong while waiting for Stream " + streamName);
            e.printStackTrace();
        }
        return streamDescription;
    }

    /**
     * Gets the shards available from a specific stream
     * @param streamName stream name
     * @param credentials AWS credentials
     * @param region AWS region
     * @return List
     */
    public static List<Shard> getShardsStream(String streamName, String credentials, String region) {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        AmazonKinesisClient kClient = getKinesisClient(credentials, region);
        describeStreamRequest.setStreamName( streamName );
        List<Shard> shards = new ArrayList<>();
        String exclusiveStartShardId = null;
        do {
            describeStreamRequest.setExclusiveStartShardId( exclusiveStartShardId );
            DescribeStreamResult describeStreamResult = kClient.describeStream( describeStreamRequest );
            shards.addAll( describeStreamResult.getStreamDescription().getShards() );
            if (describeStreamResult.getStreamDescription().getHasMoreShards() && shards.size() > 0) {
                exclusiveStartShardId = shards.get(shards.size() - 1).getShardId();
            } else {
                exclusiveStartShardId = null;
            }
        } while ( exclusiveStartShardId != null );
        return shards;
    }

    /**
     * Verifies if a Kinesis stream exists or not, and creates it.
     * @param streamName Stream Name
     * @param kinesis AmazonKinesisClient
     * @param numShards AmazonKinesis number of shards
     * @return boolean
     * @throws InterruptedException while checking AWS resources
     */
    public static boolean checkOrCreate(String streamName, AmazonKinesisClient kinesis, int numShards) throws InterruptedException {
        // Describe the stream and check if it exists.
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(streamName);
        try {
            StreamDescription streamDescription = kinesis.describeStream(describeStreamRequest).getStreamDescription();
            LOG.info("Stream " + streamName + " has a status of " + streamDescription.getStreamStatus());
            LOG.info("Stream " + streamName + " has " + streamDescription.getShards().size() + " shards");

            if ("DELETING".equals(streamDescription.getStreamStatus())) {
                LOG.error("Stream is being deleted. Please check it out.");
                return false;
            }

            // Wait for the stream to become active if it is not yet ACTIVE.
            if (!"ACTIVE".equals(streamDescription.getStreamStatus())) {
                waitForStreamToBecomeAvailable(streamName, kinesis);
            }
        } catch (ResourceNotFoundException ex) {
            LOG.info("Stream " + streamName + " does not exist. Creating it now.");

            // Create a stream. The number of shards determines the provisioned throughput.
            CreateStreamRequest createStreamRequest = new CreateStreamRequest();

            createStreamRequest.setStreamName(streamName);
            createStreamRequest.setShardCount(numShards);

            kinesis.createStream(createStreamRequest);
            // The stream is now being created. Wait for it to become active.
            waitForStreamToBecomeAvailable(streamName, kinesis);
        }
        return true;
    }

    /**
     * Gets an AmazonKinesisClient based on credentials and region
     * @param credentialsPath AWS credentials file path
     * @param region AWS region
     * @return AmazonKinesisClient
     */
    public static AmazonKinesisClient getKinesisClient(String credentialsPath, String region) {
        AWSCredentialsProvider credentialsProvider = loadAwsCredentials(credentialsPath);
        AmazonKinesisClient kClient = new AmazonKinesisClient(credentialsProvider);
        kClient.configureRegion(Regions.valueOf(region.toUpperCase()));
        return kClient;
    }

    /**
     * Waits until a stream becomes available.
     * @param streamName AmazonKinesis stream name
     * @param kinesis AmazonKinesis client
     * @throws InterruptedException
     */
    private static void waitForStreamToBecomeAvailable(String streamName, AmazonKinesisClient kinesis) throws InterruptedException {
        LOG.info("Waiting for " + streamName + " to become ACTIVE.");

        long startTime = System.currentTimeMillis();
        long endTime = startTime + TimeUnit.MINUTES.toMillis(10);
        while (System.currentTimeMillis() < endTime) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(20));

            try {
                DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
                describeStreamRequest.setStreamName(streamName);
                // ask for no more than 10 shards at a time -- this is an optional parameter
                describeStreamRequest.setLimit(10);
                DescribeStreamResult describeStreamResponse = kinesis.describeStream(describeStreamRequest);

                String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
                LOG.debug("\t- current state: " + streamStatus);
                if ("ACTIVE".equals(streamStatus)) {
                    return;
                }
            } catch (ResourceNotFoundException ex) {
                // ResourceNotFound means the stream doesn't exist yet,
                // so ignore this error and just keep polling.
                LOG.debug("Waiting for Stream " + streamName);
            } catch (AmazonServiceException ase) {
                throw ase;
            }
        }
        throw new RuntimeException(String.format("Stream %s never became active", streamName));
    }

    /**
     * Tries loading AwsCredentials.
     *
     * @param awsCredentialsPath AWS credentials file path.
     * @return AWSCredentialsProvider
     */
    public static AWSCredentialsProvider loadAwsCredentials(String awsCredentialsPath) {
        AWSCredentialsProvider credentials = null;
        try {
            credentials = awsCredentialsPath == null ? new DefaultAWSCredentialsProviderChain() :
                    new PropertiesFileCredentialsProvider(awsCredentialsPath);
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from provided path. " +
                            "Check your credential profiles file (~/.aws/credentials), or the provided path.",
                    e);
        }
        return credentials;
    }
}
