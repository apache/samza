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

package org.apache.samza.system.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import kafka.admin.AdminClient;
import kafka.utils.ZkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.KafkaConfig;
import org.apache.samza.config.SystemConfig;
import org.apache.samza.system.ExtendedSystemAdmin;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.StreamValidationException;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.ExponentialSleepStrategy;
import org.apache.samza.util.ScalaJavaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function0;
import scala.Function1;
import scala.Function2;
import scala.Option;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.BoxedUnit;

import static org.apache.kafka.clients.consumer.KafkaConsumerConfig.*;


public class SamzaKafkaSystemAdmin<K, V> implements ExtendedSystemAdmin {
  private final String systemName;
  private Consumer<K, V> metadataConsumer = null;

  private final Supplier<ZkUtils> connectZk;
  private final Supplier<AdminClient> connectAdminClient;

  //Custom properties to use when the system admin tries to create a new
  //coordinator stream.
  private final Properties coordinatorStreamProperties;

  //The replication factor to use when the system admin creates a new
  // coordinator stream.
  private final int coordinatorStreamReplicationFactor;

  // Replication factor for the Changelog topic in kafka
  //Kafka properties to be used during the Changelog topic creation
  private final Map<String, ChangelogInfo> changelogTopicMetaInformation;

  // Kafka properties to be used during the intermediate topic creation
  private final Map<String, Properties> intermediateStreamProperties;

  private static final Logger LOG = LoggerFactory.getLogger(SamzaKafkaSystemAdmin.class);

  private final KafkaSystemAdminUtilsScala kafkaAdminUtils;

  volatile private AdminClient adminClient = null;

  private final boolean deleteCommittedMessages;

  @VisibleForTesting
  public static volatile boolean deleteMessageCalled = false;

  // The same default exponential sleep strategy values as in open source
  private static final double DEFAULT_EXPONENTIAL_SLEEP_BACK_OFF_MULTIPLIER = 2.0;
  private static final long DEFAULT_EXPONENTIAL_SLEEP_INITIAL_DELAY_MS = 500;
  private static final long DEFAULT_EXPONENTIAL_SLEEP_MAX_DELAY_MS = 10000;
  private static final int MAX_RETRIES_ON_EXCEPTION = 5;

  @Override
  public void start() {
    // Plese note. There is slight inconsistency in the use of this class.
    // Some of the functinality of this class may actually be used BEFORE start() is called.
    // The SamzaContainer gets metadata (using this class) in SamzaContainer.apply, but this "start" actually gets called in SamzaContainer.run.
    // Also we assume that start is called only once.
    if (metadataConsumer == null) {
      throw new SamzaException("Cannot start SamzaKafkaSystemAdmin with null metadataConsumer");
    }

    if (adminClient == null) {
      throw new SamzaException("Cannot start SamzaKafkaSystemAdmin with null adminClient");
    }
  }

  @Override
  public void stop() {
    if (metadataConsumer != null) {
      metadataConsumer.close();
    } else {
      LOG.warn("In Stop KafkaSystemAdmin: metadataConsumer is null");
    }
    metadataConsumer = null;
    if (adminClient != null) {
      adminClient.close();
    } else {
      LOG.warn("In Stop KafkaSystemAdmin: adminClient is null");
    }
    adminClient = null;
  }

  @VisibleForTesting
  /**
   *
   * Replication factor for the Changelog topic in kafka
   * Kafka properties to be used during the Changelog topic creation
   *
   */
  SamzaKafkaSystemAdmin(String systemName, Supplier<Consumer<K, V>> metadataConsumerSupplier,
      Supplier<ZkUtils> connectZk, Supplier<AdminClient> connectAdminClient,
      Map<String, ChangelogInfo> changelogTopicMetaInformation, Map<String, Properties> intermediateStreamProperties,
      Properties coordinatorStreamProperties, int coordinatorStreamReplicationFactor,
      boolean deleteCommittedMessages) {
    this.systemName = systemName;
    this.metadataConsumer = metadataConsumerSupplier.get();
    this.adminClient = connectAdminClient.get();
    this.changelogTopicMetaInformation = changelogTopicMetaInformation;
    this.intermediateStreamProperties = intermediateStreamProperties;
    this.coordinatorStreamProperties = coordinatorStreamProperties;
    this.coordinatorStreamReplicationFactor = coordinatorStreamReplicationFactor;
    this.connectZk = connectZk;
    this.connectAdminClient = connectAdminClient;
    this.deleteCommittedMessages = deleteCommittedMessages;

    // TODO move to org.apache.kafka.clients.admin.AdminClien from the kafka.admin.AdminClient
    // when deleteMessagesBefore is available there. Then we can get rid of this KafkaSystemAdminUtilScala
    kafkaAdminUtils = new KafkaSystemAdminUtilsScala(systemName);
  }

  public static SamzaKafkaSystemAdmin getKafkaSystemAdmin(final String systemName, final Config config,
      final String idPrefix) {

    boolean zkSecure = false; // needs to be added to the argument if ever true is possible

    // admin requires adminClient for deleteBeforeMessages
    Properties props = new Properties();
    String brokerListString =
        config.get(String.format("systems.%s.consumer.%s", systemName, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    if (brokerListString == null) {
      brokerListString =
          config.get(String.format("systems.%s.producer.%s", systemName, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }
    if (brokerListString == null) {
      throw new SamzaException("" + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG + " is required for systemAdmin for system " + systemName );
    }
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerListString);

    // kafka.admin.AdminUtils requires zkConnect
    // this will change after we move to the new org.apache..AdminClient
    String zkConnectStr = config.get(String.format("systems.%s.consumer.%s", systemName, ZOOKEEPER_CONNECT));
    if (zkConnectStr == null) {
      throw new SamzaException("Missing zookeeper.connect config for admin for system " + systemName);
    }
    props.put(ZOOKEEPER_CONNECT, zkConnectStr);

    Supplier<AdminClient> adminClientSupplier = () -> {
      return AdminClient.create(props);
    };

    Supplier<ZkUtils> zkConnectSupplier = () -> {
      return ZkUtils.apply(zkConnectStr, 6000, 6000, zkSecure);
    };
    // extract kafka client configs
    KafkaConsumerConfig consumerConfig =
        KafkaConsumerConfig.getKafkaSystemConsumerConfig(config, systemName, idPrefix, Collections.emptyMap());


    // KafkaConsumer for metadata access
    Supplier<Consumer<byte[], byte[]>> metadataConsumerSupplier =
        () -> KafkaSystemConsumer.getKafkaConsumerImpl(systemName, consumerConfig);

    KafkaConfig kafkaConfig = new KafkaConfig(config);
    Properties coordinatorStreamProperties = KafkaSystemAdminUtilsScala.getCoordinatorTopicProperties(kafkaConfig);
    int coordinatorStreamReplicationFactor = Integer.valueOf(kafkaConfig.getCoordinatorReplicationFactor());

    Map<String, String> storeToChangelog =
        JavaConverters.mapAsJavaMapConverter(kafkaConfig.getKafkaChangelogEnabledStores()).asJava();
    // Construct the meta information for each topic, if the replication factor is not defined,
    // we use 2 as the number of replicas for the change log stream.
    Map<String, ChangelogInfo> topicMetaInformation = new HashMap<>();
    for (Map.Entry<String, String> e : storeToChangelog.entrySet()) {
      String storeName = e.getKey();
      String topicName = e.getValue();
      String replicationFactorStr = kafkaConfig.getChangelogStreamReplicationFactor(storeName);
      int replicationFactor = StringUtils.isEmpty(replicationFactorStr) ? 2 : Integer.valueOf(replicationFactorStr);
      ChangelogInfo changelogInfo =
          new ChangelogInfo(replicationFactor, kafkaConfig.getChangelogKafkaProperties(storeName));
      LOG.info(String.format("Creating topic meta information for topic: %s with replication factor: %s", topicName,
          replicationFactor));
      topicMetaInformation.put(topicName, changelogInfo);
    }

    // special flag to allow/enforce deleting of committed messages
    SystemConfig systemConfig = new SystemConfig(config);
    Option<String> deleteCommittedMessagesOpt = systemConfig.deleteCommittedMessages(systemName);
    boolean deleteCommittedMessages =
        (deleteCommittedMessagesOpt.isEmpty()) ? false : Boolean.valueOf(deleteCommittedMessagesOpt.get());

    Map<String, Properties> intermediateStreamProperties =
        JavaConverters.mapAsJavaMapConverter(KafkaSystemAdminUtilsScala.getIntermediateStreamProperties(config))
            .asJava();

    LOG.info(String.format("Creating kafka Admin for system %s, idPrefix %s", systemName, idPrefix));

    return new SamzaKafkaSystemAdmin(systemName, metadataConsumerSupplier, zkConnectSupplier, adminClientSupplier,
        topicMetaInformation, intermediateStreamProperties, coordinatorStreamProperties,
        coordinatorStreamReplicationFactor, deleteCommittedMessages);
  }

  @Override
  public Map<String, SystemStreamMetadata> getSystemStreamPartitionCounts(Set<String> streamNames, long cacheTTL) {
    // This optimization omits actual metadata for performance. Instead, we inject a dummy for all partitions.
    final SystemStreamMetadata.SystemStreamPartitionMetadata dummySspm =
        new SystemStreamMetadata.SystemStreamPartitionMetadata(null, null, null);

    ExponentialSleepStrategy strategy = new ExponentialSleepStrategy(DEFAULT_EXPONENTIAL_SLEEP_BACK_OFF_MULTIPLIER,
        DEFAULT_EXPONENTIAL_SLEEP_INITIAL_DELAY_MS, DEFAULT_EXPONENTIAL_SLEEP_MAX_DELAY_MS);

    Function1<ExponentialSleepStrategy.RetryLoop, Map<String, SystemStreamMetadata>> fetchMetadataOperation =
        new AbstractFunction1<ExponentialSleepStrategy.RetryLoop, Map<String, SystemStreamMetadata>>() {
          @Override
          public Map<String, SystemStreamMetadata> apply(ExponentialSleepStrategy.RetryLoop loop) {
            Map<String, SystemStreamMetadata> allMetadata = new HashMap<>();

            streamNames.forEach(streamName -> {
              Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> partitionMetadata = new HashMap<>();

              List<PartitionInfo> partitionInfos = metadataConsumer.partitionsFor(streamName);
              LOG.debug("Stream {} has partitions {}", streamName, partitionInfos);

              partitionInfos.forEach(partitionInfo -> {
                partitionMetadata.put(new Partition(partitionInfo.partition()), dummySspm);
              });

              allMetadata.put(streamName, new SystemStreamMetadata(streamName, partitionMetadata));
            });

            loop.done();
            return allMetadata;
          }
        };

    Map<String, SystemStreamMetadata> result = strategy.run(fetchMetadataOperation,
        new AbstractFunction2<Exception, ExponentialSleepStrategy.RetryLoop, BoxedUnit>() {
          @Override
          public BoxedUnit apply(Exception exception, ExponentialSleepStrategy.RetryLoop loop) {
            if (loop.sleepCount() < MAX_RETRIES_ON_EXCEPTION) {
              LOG.warn(String.format("Fetching systemstreampartition counts for: %s threw an exception. Retrying.",
                  streamNames), exception);
            } else {
              LOG.error(String.format("Fetching systemstreampartition counts for: %s threw an exception.", streamNames),
                  exception);
              loop.done();
              throw new SamzaException(exception);
            }
            return null;
          }
        }).get();

    return result;
  }

  @Override
  public Map<SystemStreamPartition, String> getOffsetsAfter(Map<SystemStreamPartition, String> offsets) {
    // This is safe to do with Kafka, even if a topic is key-deduped. If the
    // offset doesn't exist on a compacted topic, Kafka will return the first
    // message AFTER the offset that was specified in the fetch request.
    return offsets.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, (entry) -> String.valueOf(Long.valueOf(entry.getValue()) + 1)));
  }

  @Override
  public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
    return getSystemStreamMetadata(streamNames,
        new ExponentialSleepStrategy(DEFAULT_EXPONENTIAL_SLEEP_BACK_OFF_MULTIPLIER,
            DEFAULT_EXPONENTIAL_SLEEP_INITIAL_DELAY_MS, DEFAULT_EXPONENTIAL_SLEEP_MAX_DELAY_MS));
  }

  @Override
  public Map<SystemStreamPartition, SystemStreamMetadata.SystemStreamPartitionMetadata> getSSPMetadata(
      Set<SystemStreamPartition> ssps) {

    LOG.info("Fetching SSP metadata for: {}", ssps);
    List<TopicPartition> topicPartitions = ssps.stream()
        .map(ssp -> new TopicPartition(ssp.getStream(), ssp.getPartition().getPartitionId()))
        .collect(Collectors.toList());

    OffsetsMaps topicPartitionsMetadata;
    topicPartitionsMetadata = fetchTopicPartitionsMetadata(topicPartitions);

    Map<SystemStreamPartition, SystemStreamMetadata.SystemStreamPartitionMetadata> sspToSSPMetadata = new HashMap<>();
    for (SystemStreamPartition ssp : ssps) {
      String oldestOffset = topicPartitionsMetadata.getOldestOffsets().get(ssp);
      String newestOffset = topicPartitionsMetadata.getNewestOffsets().get(ssp);
      String upcomingOffset = topicPartitionsMetadata.getUpcomingOffsets().get(ssp);
      if (oldestOffset != null || newestOffset != null || upcomingOffset != null) {
        sspToSSPMetadata.put(ssp,
            new SystemStreamMetadata.SystemStreamPartitionMetadata(oldestOffset, newestOffset, upcomingOffset));
      }
    }
    return sspToSSPMetadata;
  }

  /**
   * Given a set of stream names (topics), fetch metadata from Kafka for each
   * stream, and return a map from stream name to SystemStreamMetadata for
   * each stream. This method will return null for oldest and newest offsets
   * if a given SystemStreamPartition is empty. This method will block and
   * retry indefinitely until it gets a successful response from Kafka.
   *
   * @param streamNames a set of strings of stream names/topics
   * @param retryBackoff retry backoff strategy
   * @return a map from topic to SystemStreamMetadata which has offsets for each partition
   */
  public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames,
      ExponentialSleepStrategy retryBackoff) {

    LOG.info("Fetching system stream metadata for {} from system {}", streamNames, systemName);

    Function1<ExponentialSleepStrategy.RetryLoop, Map<String, SystemStreamMetadata>> fetchMetadataOperation =
        new AbstractFunction1<ExponentialSleepStrategy.RetryLoop, Map<String, SystemStreamMetadata>>() {
          @Override
          public Map<String, SystemStreamMetadata> apply(ExponentialSleepStrategy.RetryLoop loop) {
            Map<String, SystemStreamMetadata> metadata = fetchSystemStreamMetadata(streamNames);
            loop.done();
            return metadata;
          }
        };

    Function2<Exception, ExponentialSleepStrategy.RetryLoop, BoxedUnit> onExceptionRetryOperation =
        new AbstractFunction2<Exception, ExponentialSleepStrategy.RetryLoop, BoxedUnit>() {
          @Override
          public BoxedUnit apply(Exception exception, ExponentialSleepStrategy.RetryLoop loop) {
            if (loop.sleepCount() < MAX_RETRIES_ON_EXCEPTION) {
              LOG.warn(
                  String.format("Fetching system stream metadata for: %s threw an exception. Retrying.", streamNames),
                  exception);
            } else {
              LOG.error(String.format("Fetching system stream metadata for: %s threw an exception.", streamNames),
                  exception);
              loop.done();
              throw new SamzaException(exception);
            }

            return null;
          }
        };

    Function0<Map<String, SystemStreamMetadata>> fallbackOperation =
        new AbstractFunction0<Map<String, SystemStreamMetadata>>() {
          @Override
          public Map<String, SystemStreamMetadata> apply() {
            throw new SamzaException("Failed to get system stream metadata");
          }
        };

    Map<String, SystemStreamMetadata> result =
        retryBackoff.run(fetchMetadataOperation, onExceptionRetryOperation).getOrElse(fallbackOperation);
    return result;
  }

  @Override
  public String getNewestOffset(SystemStreamPartition ssp, Integer maxRetries) {
    LOG.info("Fetching newest offset for: {}", ssp);

    ExponentialSleepStrategy strategy = new ExponentialSleepStrategy(DEFAULT_EXPONENTIAL_SLEEP_BACK_OFF_MULTIPLIER,
        DEFAULT_EXPONENTIAL_SLEEP_INITIAL_DELAY_MS, DEFAULT_EXPONENTIAL_SLEEP_MAX_DELAY_MS);

    Function1<ExponentialSleepStrategy.RetryLoop, String> fetchNewestOffset =
        new AbstractFunction1<ExponentialSleepStrategy.RetryLoop, String>() {
          @Override
          public String apply(ExponentialSleepStrategy.RetryLoop loop) {
            String result = fetchNewestOffset(ssp);
            loop.done();
            return result;
          }
        };

    String offset = strategy.run(fetchNewestOffset,
        new AbstractFunction2<Exception, ExponentialSleepStrategy.RetryLoop, BoxedUnit>() {
          @Override
          public BoxedUnit apply(Exception exception, ExponentialSleepStrategy.RetryLoop loop) {
            if (loop.sleepCount() < maxRetries) {
              LOG.warn(String.format("Fetching newest offset for: %s threw an exception. Retrying.", ssp), exception);
            } else {
              LOG.error(String.format("Fetching newest offset for: %s threw an exception.", ssp), exception);
              loop.done();
              throw new SamzaException("Exception while trying to get newest offset", exception);
            }
            return null;
          }
        }).get();

    return offset;
  }

  /**
   * convert TopicPartition to SystemStreamPartition
   * @param topicPartition the topic partition to be created
   * @return an instance of SystemStreamPartition
   */
  private SystemStreamPartition createSystemStreamPartition(TopicPartition topicPartition) {
    String topic = topicPartition.topic();
    Partition partition = new Partition(topicPartition.partition());
    return new SystemStreamPartition(systemName, topic, partition);
  }

  /**
   * Uses {@code metadataConsumer} to fetch the metadata for the {@code topicPartitions}.
   * Warning: If multiple threads call this with the same {@code metadataConsumer}, then this will not protect against
   * concurrent access to the {@code metadataConsumer}.
   */
  private OffsetsMaps fetchTopicPartitionsMetadata(List<TopicPartition> topicPartitions) {
    Map<SystemStreamPartition, String> oldestOffsets = new HashMap<>();
    Map<SystemStreamPartition, String> newestOffsets = new HashMap<>();
    Map<SystemStreamPartition, String> upcomingOffsets = new HashMap<>();

    Map<TopicPartition, Long> oldestOffsetsWithLong = metadataConsumer.beginningOffsets(topicPartitions);
    LOG.debug("Kafka-fetched beginningOffsets: {}", oldestOffsetsWithLong);
    Map<TopicPartition, Long> upcomingOffsetsWithLong = metadataConsumer.endOffsets(topicPartitions);
    LOG.debug("Kafka-fetched endOffsets: {}", upcomingOffsetsWithLong);

    oldestOffsetsWithLong.forEach((topicPartition, offset) -> {
      oldestOffsets.put(createSystemStreamPartition(topicPartition), String.valueOf(offset));
    });

    upcomingOffsetsWithLong.forEach((topicPartition, offset) -> {
      upcomingOffsets.put(createSystemStreamPartition(topicPartition), String.valueOf(offset));

      // Kafka's beginning Offset corresponds to the offset for the oldest message.
      // Kafka's end offset corresponds to the offset for the upcoming message, and it is the newest offset + 1.
      // When upcoming offset is <=0, the topic appears empty, we put oldest offset 0 and the newest offset null.
      // When upcoming offset is >0, we subtract the upcoming offset by one for the newest offset.
      // For normal case, the newest offset will correspond to the offset of the newest message in the stream;
      // But for the big message, it is not the case. Seeking on the newest offset gives nothing for the newest big message.
      // For now, we keep it as is for newest offsets the same as historical metadata structure.
      if (offset <= 0) {
        LOG.debug(
            "Empty Kafka topic partition {} with upcoming offset {}. Skipping newest offset and setting oldest offset to 0 to consume from beginning",
            topicPartition, offset);
        oldestOffsets.put(createSystemStreamPartition(topicPartition), "0");
      } else {
        newestOffsets.put(createSystemStreamPartition(topicPartition), String.valueOf(offset - 1));
      }
    });
    return new OffsetsMaps(oldestOffsets, newestOffsets, upcomingOffsets);
  }

  /**
   * Fetch SystemStreamMetadata for each topic with the consumer
   * @param topics
   * @return scala map of topic to SystemStreamMetadata
   */
  private Map<String, SystemStreamMetadata> fetchSystemStreamMetadata(Set<String> topics) {
    Map<SystemStreamPartition, String> allOldestOffsets = new HashMap<>();
    Map<SystemStreamPartition, String> allNewestOffsets = new HashMap<>();
    Map<SystemStreamPartition, String> allUpcomingOffsets = new HashMap<>();

    LOG.info("fetching SystemStreamMetadata for topics {} on system {}", topics, systemName);

    topics.forEach(topic -> {
      List<PartitionInfo> partitionInfos = metadataConsumer.partitionsFor(topic);

      List<TopicPartition> topicPartitions = partitionInfos.stream()
          .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
          .collect(Collectors.toList());

      OffsetsMaps offsetsForTopic = fetchTopicPartitionsMetadata(topicPartitions);
      allOldestOffsets.putAll(offsetsForTopic.getOldestOffsets());
      allNewestOffsets.putAll(offsetsForTopic.getNewestOffsets());
      allUpcomingOffsets.putAll(offsetsForTopic.getUpcomingOffsets());
    });

    scala.collection.immutable.Map<String, SystemStreamMetadata> result =
        KafkaSystemAdminUtilsScala.assembleMetadata(ScalaJavaUtil.toScalaMap(allOldestOffsets),
            ScalaJavaUtil.toScalaMap(allNewestOffsets), ScalaJavaUtil.toScalaMap(allUpcomingOffsets));

    LOG.info("assembled SystemStreamMetadata is: {}", result);
    return JavaConverters.mapAsJavaMapConverter(result).asJava();
  }

  private String fetchNewestOffset(SystemStreamPartition ssp) {
    LOG.debug("Fetching newest offset for {}", ssp);
    String newestOffset;

    // create a kafka consumer for fetching meta data
    TopicPartition topicPartition = new TopicPartition(ssp.getStream(), ssp.getPartition().getPartitionId());
    List<TopicPartition> topicPartitions = Collections.singletonList(topicPartition);

    // the offsets returned from the consumer is the Long type
    Long upcomingOffset = (Long) metadataConsumer.endOffsets(topicPartitions).get(topicPartition);

    // Kafka's "latest" offset is always last message in stream's offset + 1,
    // so get newest message in stream by subtracting one. this is safe
    // even for key-deduplicated streams, since the last message will
    // never be deduplicated.
    if (upcomingOffset <= 0) {
      LOG.debug("Stripping newest offsets for {} because the topic appears empty.", topicPartition);
      newestOffset = null;
    } else {
      newestOffset = String.valueOf(upcomingOffset - 1);
    }

    LOG.debug("Newest offset for ssp {} is: {}", ssp, newestOffset);
    return newestOffset;
  }

  @Override
  public Integer offsetComparator(String offset1, String offset2) {
    if (offset1 == null || offset2 == null) {
      return -1;
    }

    return Long.valueOf(offset1).compareTo(Long.valueOf(offset2));
  }

  @Override
  public boolean createStream(StreamSpec streamSpec) {
    LOG.info("Creating Kafka topic: {} on system: {}", streamSpec.getPhysicalName(), streamSpec.getSystemName());

    return kafkaAdminUtils.createStream(toKafkaSpec(streamSpec), connectZk);
  }

  @Override
  public boolean clearStream(StreamSpec streamSpec) {
    LOG.info("Creating Kafka topic: {} on system: {}", streamSpec.getPhysicalName(), streamSpec.getSystemName());

    kafkaAdminUtils.clearStream(streamSpec, connectZk);

    Map<String, List<PartitionInfo>> topicsMetadata = getTopicMetadata(ImmutableSet.of(streamSpec.getPhysicalName()));
    return topicsMetadata.get(streamSpec.getPhysicalName()).isEmpty();
  }

  /**
   * Converts a StreamSpec into a KafakStreamSpec. Special handling for coordinator and changelog stream.
   * @param spec a StreamSpec object
   * @return KafkaStreamSpec object
   */
  KafkaStreamSpec toKafkaSpec(StreamSpec spec) {
    KafkaStreamSpec kafkaSpec;
    if (spec.isChangeLogStream()) {
      String topicName = spec.getPhysicalName();
      ChangelogInfo topicMeta = changelogTopicMetaInformation.get(topicName);
      if (topicMeta == null) {
        throw new StreamValidationException("Unable to find topic information for topic " + topicName);
      }

      kafkaSpec = new KafkaStreamSpec(spec.getId(), topicName, systemName, spec.getPartitionCount(),
          topicMeta.replicationFactor(), topicMeta.kafkaProps());
    } else if (spec.isCoordinatorStream()) {
      kafkaSpec =
          new KafkaStreamSpec(spec.getId(), spec.getPhysicalName(), systemName, 1, coordinatorStreamReplicationFactor,
              coordinatorStreamProperties);
    } else if (intermediateStreamProperties.containsKey(spec.getId())) {
      kafkaSpec = KafkaStreamSpec.fromSpec(spec).copyWithProperties(intermediateStreamProperties.get(spec.getId()));
    } else {
      kafkaSpec = KafkaStreamSpec.fromSpec(spec);
    }
    return kafkaSpec;
  }

  @Override
  public void validateStream(StreamSpec streamSpec) throws StreamValidationException {
    LOG.info("About to validate stream = " + streamSpec);

    String streamName = streamSpec.getPhysicalName();
    SystemStreamMetadata systemStreamMetadata =
        getSystemStreamMetadata(Collections.singleton(streamName)).get(streamName);
    if (systemStreamMetadata == null) {
      throw new StreamValidationException(
          "Failed to obtain metadata for stream " + streamName + ". Validation failed.");
    }

    int actualPartitionCounter = systemStreamMetadata.getSystemStreamPartitionMetadata().size();
    int expectedPartitionCounter = streamSpec.getPartitionCount();
    LOG.info("actualCount=" + actualPartitionCounter + "; expectedCount=" + expectedPartitionCounter);
    if (actualPartitionCounter != expectedPartitionCounter) {
      throw new StreamValidationException(
          String.format("Mismatch of partitions for stream %s. Expected %d, got %d. Validation failed.", streamName,
              expectedPartitionCounter, actualPartitionCounter));
    }
  }

  /**
   * get partition info for topic
   * @param topics set of topics to query
   */
  Map<String, List<PartitionInfo>> getTopicMetadata(Set<String> topics) {
    // me
    final Map<String, List<PartitionInfo>> listPartitionsInfo = new HashMap();
    List<PartitionInfo> l;
    for (String topic : topics) {
      l = metadataConsumer.partitionsFor(topic);
      listPartitionsInfo.put(topic, l);
    }

    return listPartitionsInfo;
  }

  /**
   *
   * Delete records up to (and including) the provided ssp offsets for all system stream partitions specified in the map
   * This only works with Kafka cluster 0.11 or later. Otherwise it's a no-op.
   */
  @Override
  public void deleteMessages(Map<SystemStreamPartition, String> offsets) {
    if (adminClient == null) {
      throw new SamzaException("KafkaSystemAdmin has not started yet for system " + systemName);
    }
    if (deleteCommittedMessages) {
      KafkaSystemAdminUtilsScala.deleteMessages(adminClient, offsets);
      deleteMessageCalled = true;
    }
  }

  /**
   * Container for metadata about offsets.
   */
  private static class OffsetsMaps {
    private final Map<SystemStreamPartition, String> oldestOffsets;
    private final Map<SystemStreamPartition, String> newestOffsets;
    private final Map<SystemStreamPartition, String> upcomingOffsets;

    private OffsetsMaps(Map<SystemStreamPartition, String> oldestOffsets,
        Map<SystemStreamPartition, String> newestOffsets, Map<SystemStreamPartition, String> upcomingOffsets) {
      this.oldestOffsets = oldestOffsets;
      this.newestOffsets = newestOffsets;
      this.upcomingOffsets = upcomingOffsets;
    }

    private Map<SystemStreamPartition, String> getOldestOffsets() {
      return oldestOffsets;
    }

    private Map<SystemStreamPartition, String> getNewestOffsets() {
      return newestOffsets;
    }

    private Map<SystemStreamPartition, String> getUpcomingOffsets() {
      return upcomingOffsets;
    }
  }
}
