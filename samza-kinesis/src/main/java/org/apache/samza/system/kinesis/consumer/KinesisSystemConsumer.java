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

package org.apache.samza.system.kinesis.consumer;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.*;
import org.apache.samza.system.kinesis.KinesisUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.BlockingEnvelopeMap;
import org.apache.samza.util.DaemonThreadFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.samza.system.kinesis.Constants.*;

/**
 * Implements Samza {@link org.apache.samza.system.SystemConsumer} interface using a queue.
 * The low level Amazon Kinesis API is used to allow Samza to control fail-recovery and
 * better mapping among container, messages and tasks.
 */
public class KinesisSystemConsumer extends BlockingEnvelopeMap {
    private static final String KINESIS_CONSUMER_SYSTEM_THREAD_PREFIX = "kinesis-";
    /**
     * AWS credentials
     */
    private static AmazonKinesisClient kClient;
    /**
     * App name
     */
    private final String appName;
    /**
     * Region where the Kinesis streams are
     */
    private final String region;
    /**
     * Logger for the KinesisSystemConsumer
     */
    private static final Log LOG = LogFactory.getLog(KinesisSystemConsumer.class);
    /**
     * System name
     */
    private final String systemName;
    /**
     * Initial Kinesis stream position
     */
    private String initialPos;
    private String sequenceNumber;
    private int requestRecords;
    private static Map<SystemStreamPartition, String> sspShardIteratorMap = new HashMap<>();

    /**
     * Message sequence numbers delivered per partition since the last checkpoint.
     */
    private Map<SystemStreamPartition, Queue<Delivery>> deliveries =
            new ConcurrentHashMap<>();

    /**
     * Constructor
     *
     * @param systemName System name
     * @param config System configuration
     */
    public KinesisSystemConsumer(String systemName, Config config) {
        String awsCredentialsPath = config.get(String.format("systems.%s.%s", systemName, CONFIG_PATH_PARAM));
        String iniPos = config.get(String.format("systems.%s.%s", systemName, STREAM_POSITION_PARAM));
        //TODO not yet used
        String seqNumber = config.get(String.format("systems.%s.%s", systemName, SEQUENCE_NUMBER_PARAM));
        String region = config.get(String.format("systems.%s.%s", systemName, AWS_REGION_PARAM));
        int reqRecords = config.getInt(String.format("systems.%s.%s", systemName, MAX_REQUEST_RECORDS_PARAM), 0);
        String appName = config.get("job.name");

        this.systemName = systemName;
        this.appName = appName;
        this.initialPos = iniPos;
        this.sequenceNumber = seqNumber!=null?seqNumber:"";
        this.region = region.toUpperCase();
        this.requestRecords = reqRecords;
        this.kClient = KinesisUtils.getKinesisClient(awsCredentialsPath, region);
    }

    /**
     * For multiple input streams, Samza calls this method once for each of them.
     * Samza still controls on how to do fail-recovery and re-tries by mapping
     * each Samza partition to a Kinesis shard.
     */
    @Override
    public void register(SystemStreamPartition systemStreamPartition, String offset) {
        super.register(systemStreamPartition, offset);

        String partId = String.valueOf(systemStreamPartition.getPartition().getPartitionId());
        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
        getShardIteratorRequest.setStreamName(systemStreamPartition.getStream());
        getShardIteratorRequest.setShardId(SHARDID_PREFFIX.format(partId));
        getShardIteratorRequest.setShardIteratorType(initialPos);
        if (!sequenceNumber.isEmpty() && initialPos.endsWith(SEQUENCE_NUMBER_SUFFIX)) {
            LOG.warn("Reading stream from sequence number %s.".format(sequenceNumber));
            getShardIteratorRequest.setStartingSequenceNumber(sequenceNumber);
        }

        GetShardIteratorResult getShardIteratorResult = kClient.getShardIterator(getShardIteratorRequest);
        // initial shardIterator value
        String shardIterator = getShardIteratorResult.getShardIterator();
        sspShardIteratorMap.put(systemStreamPartition, shardIterator);
    }

    private ExecutorService pool;

    @Override
    public void start() {
        pool = Executors.newFixedThreadPool(sspShardIteratorMap.size(), new DaemonThreadFactory(KINESIS_CONSUMER_SYSTEM_THREAD_PREFIX));
        for (Map.Entry<SystemStreamPartition, String> entry : sspShardIteratorMap.entrySet()) {
            pool.execute(readShardRecords(entry.getKey(), entry.getValue()));
        }
    }

    private Runnable readShardRecords(final SystemStreamPartition ssp, final String iniShardIterator) {
        Runnable runnable = new Runnable() {

            volatile boolean continueFlg = true;

            public void stop() {
                continueFlg = false;
            }

            @Override
            public void run() {
                // Continuously read data records from a shard
                List<Record> records;
                String shardIterator = iniShardIterator;

                while (continueFlg) {

                    // Create a new getRecordsRequest with an existing shardIterator
                    GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
                    getRecordsRequest.setShardIterator(shardIterator);
                    if (requestRecords > 0) {
                        LOG.warn("Reading a maximum of %s records per request".format(String.valueOf(requestRecords)));
                        getRecordsRequest.setLimit(requestRecords);
                    }

                    GetRecordsResult result = kClient.getRecords(getRecordsRequest);
                    try {
                        // Put the result into record list. The result can be empty.
                        for (Record record : result.getRecords()) {

                            IncomingMessageEnvelope envelope = new IncomingMessageEnvelope(ssp, record.getSequenceNumber(), record.getPartitionKey(), record.getData());
                            put(ssp, envelope);
                            trackDeliveries(Thread.currentThread().getName(), envelope);
                        }
                        // Waiting for more records
                        Thread.sleep(1000);
                    } catch (InterruptedException exception) {
                        throw new RuntimeException(exception);
                    }

                    shardIterator = result.getNextShardIterator();
                }
            }
        };
        return runnable;
    }

    @Override
    public void stop() {
        pool.shutdown();
    }

    /**
     * Method to help us keep track of messages delivered.
     *
     * @param threadName
     * @param envelope
     */
    private void trackDeliveries(String threadName, IncomingMessageEnvelope envelope) {
        if (!deliveries.containsKey(envelope.getSystemStreamPartition())) {
            deliveries.put(envelope.getSystemStreamPartition(), new ConcurrentLinkedQueue<Delivery>());
        }

        Queue<Delivery> queue = deliveries.get(envelope.getSystemStreamPartition());
        queue.add(new Delivery(envelope.getOffset(), threadName));
    }

    /**
     * Class to map between the sequenceNumbers obtained and the processors that got them
     */
    private static class Delivery {
        /**
         * Message sequence number
         */
        private final String sequenceNumber;
        /**
         * Kinesis shard reader
         */
        private final String threadName;

        /**
         * Constructor
         *
         * @param sequenceNumber
         * @param threadName
         */
        public Delivery(String sequenceNumber, String threadName) {
            this.sequenceNumber = sequenceNumber;
            this.threadName = threadName;
        }
    }

}
