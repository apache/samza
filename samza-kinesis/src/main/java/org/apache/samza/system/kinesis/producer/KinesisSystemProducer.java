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

package org.apache.samza.system.kinesis.producer;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.*;
import org.apache.samza.system.kinesis.KinesisUtils;
import org.apache.samza.config.Config;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.samza.system.kinesis.Constants.*;
import static org.apache.samza.system.kinesis.KinesisUtils.getKinesisClient;

/**
 * Class to produce streams into Kinesis' streams.
 * Implements Samza {@link org.apache.samza.system.SystemProducer}
 */
public class KinesisSystemProducer implements SystemProducer {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisSystemProducer.class);

    private final Map<String, List<PutRecordsRequestEntry>> streams;
    private AmazonKinesisClient kinesis;
    private final boolean autoCreate;
    private final int numShards;
    private final String streamName;

    public KinesisSystemProducer(String systemName, Config config) {
        String credentialsPath = config.get(String.format("systems.%s.%s", systemName, CONFIG_PATH_PARAM));
        String region = config.get(String.format("systems.%s.%s", systemName, AWS_REGION_PARAM));
        this.autoCreate = config.getBoolean(String.format("systems.%s.%s", systemName, AUTO_CREATE_STREAM), false);
        this.numShards = config.getInt(String.format("systems.%s.%s", systemName, NUMBER_SHARD), DEFAULT_NUM_SHARDS);
        this.streamName = config.get(String.format("systems.%s.%s", systemName, STREAM_NAME_PARAM));
        this.kinesis = getKinesisClient(credentialsPath, region);
        this.streams = new HashMap<>();
    }

    //TODO unit test kinesis
    @Override
    public void start() {
        // Nothing to do
    }

    @Override
    public void stop() {
        // Nothing to do
    }

    @Override
    public void register(String stream) {
        this.streams.put(stream, new LinkedList<PutRecordsRequestEntry>());
        if (this.autoCreate) {
            try {
                if (KinesisUtils.checkOrCreate(stream, kinesis, numShards))
                    LOG.debug(stream + " successfully created.");
                else
                    LOG.debug(stream + " was not successfully created.");
            } catch (InterruptedException e) {
                LOG.error("Something went wrong while trying to create " + stream);
                e.printStackTrace();
            }
        }
    }

    @Override
    public void send(String source, OutgoingMessageEnvelope outgoingMessageEnvelope) {
        PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
        // setting the key
        putRecordsRequestEntry.setPartitionKey(new String((byte[]) outgoingMessageEnvelope.getKey()));
        // setting the data
        putRecordsRequestEntry.setData(ByteBuffer.wrap((byte[]) outgoingMessageEnvelope.getMessage()));
        // adding it to be flushed later on
        this.streams.get(source).add(putRecordsRequestEntry);
        // TODO check kafka flushing
        if (this.streams.get(source).size() == 500 )
            this.flush(source);
    }

    @Override
    public void flush(String source) {
        List<PutRecordsRequestEntry> putRequestsEntryList = this.streams.get(source);
        // if there are requests to be flushed
        if (putRequestsEntryList.size() > 0) {
            LOG.debug("Flushing " + putRequestsEntryList.size() + " records.");
            PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
            putRecordsRequest.setStreamName(this.streamName);
            putRecordsRequest.setRecords(putRequestsEntryList);
            PutRecordsResult putRecordsResult = this.kinesis.putRecords(putRecordsRequest);

            // checking if there were failed requests
            if (putRecordsResult.getFailedRecordCount() > 0) {
                LOG.warn("Number of records not flushed into Kinesis: " + putRecordsResult.getFailedRecordCount());
                LOG.warn("Retrying to insert records");
            }

            // retry if errors
            while (putRecordsResult.getFailedRecordCount() > 0) {
                final List<PutRecordsRequestEntry> failedRecordsList = new ArrayList<>();
                final List<PutRecordsResultEntry> putRecordsResultEntryList = putRecordsResult.getRecords();
                LOG.debug("Retrying for " + putRecordsResultEntryList.size() + " records.");
                for (int i = 0; i < putRecordsResultEntryList.size(); i++) {
                    final PutRecordsRequestEntry putRecordRequestEntry = putRequestsEntryList.get(i);
                    final PutRecordsResultEntry putRecordsResultEntry = putRecordsResultEntryList.get(i);
                    if (putRecordsResultEntry.getErrorCode() != null) {
                        failedRecordsList.add(putRecordRequestEntry);
                    }
                }
                putRequestsEntryList = failedRecordsList;
                putRecordsRequest.setRecords(putRequestsEntryList);
                putRecordsResult = this.kinesis.putRecords(putRecordsRequest);
            }
            // clearing the to-be-flushed list
            this.streams.put(source, new LinkedList<PutRecordsRequestEntry>());
        }
    }
}