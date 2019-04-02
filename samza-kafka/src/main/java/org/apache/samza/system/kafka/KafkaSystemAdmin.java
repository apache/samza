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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.KafkaConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.config.SystemConfig;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.StreamValidationException;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.ExponentialSleepStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function0;
import scala.Function1;
import scala.Function2;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.BoxedUnit;


public class KafkaSystemAdmin implements SystemAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSystemAdmin.class);

  // Default exponential sleep strategy values
  protected static final double DEFAULT_EXPONENTIAL_SLEEP_BACK_OFF_MULTIPLIER = 2.0;
  protected static final long DEFAULT_EXPONENTIAL_SLEEP_INITIAL_DELAY_MS = 500;
  protected static final long DEFAULT_EXPONENTIAL_SLEEP_MAX_DELAY_MS = 10000;
  protected static final int MAX_RETRIES_ON_EXCEPTION = 5;
  protected static final int DEFAULT_REPL_FACTOR = 2;
  private static final int KAFKA_ADMIN_OPS_TIMEOUT_MS = 50000;

  // used in TestRepartitionJoinWindowApp TODO - remove SAMZA-1945
  @VisibleForTesting
  public static volatile boolean deleteMessageCalled = false;

  protected final String systemName;
  protected final Consumer metadataConsumer;
  protected final Config config;

  // Custom properties to create a new coordinator stream.
  private final Properties coordinatorStreamProperties;

  // Replication factor for a new coordinator stream.
  private final int coordinatorStreamReplicationFactor;

  // Replication factor and kafka properties for changelog topic creation
  private final Map<String, ChangelogInfo> changelogTopicMetaInformation;

  // Kafka properties for intermediate topics creation
  private final Map<String, Properties> intermediateStreamProperties;

  // used for intermediate streams
  protected final boolean deleteCommittedMessages;

  // admin client for create/remove topics
  final AdminClient adminClient;

  private final AtomicBoolean stopped = new AtomicBoolean(false);

  public KafkaSystemAdmin(String systemName, Config config, Consumer metadataConsumer) {
    this.systemName = systemName;
    this.config = config;

    if (metadataConsumer == null) {
      throw new SamzaException(
          "Cannot construct KafkaSystemAdmin for system " + systemName + " with null metadataConsumer");
    }
    this.metadataConsumer = metadataConsumer;

    Properties props = createAdminClientProperties();
    LOG.info("New admin client with props:" + props);
    adminClient = AdminClient.create(props);

    KafkaConfig kafkaConfig = new KafkaConfig(config);
    coordinatorStreamReplicationFactor = Integer.valueOf(kafkaConfig.getCoordinatorReplicationFactor());
    coordinatorStreamProperties = getCoordinatorStreamProperties(kafkaConfig);

    Map<String, String> storeToChangelog =
        JavaConverters.mapAsJavaMapConverter(kafkaConfig.getKafkaChangelogEnabledStores()).asJava();
    // Construct the meta information for each topic, if the replication factor is not defined,
    // we use 2 (DEFAULT_REPL_FACTOR) as the number of replicas for the change log stream.
    changelogTopicMetaInformation = new HashMap<>();
    for (Map.Entry<String, String> e : storeToChangelog.entrySet()) {
      String storeName = e.getKey();
      String topicName = e.getValue();
      String replicationFactorStr = kafkaConfig.getChangelogStreamReplicationFactor(storeName);
      int replicationFactor =
          StringUtils.isEmpty(replicationFactorStr) ? DEFAULT_REPL_FACTOR : Integer.valueOf(replicationFactorStr);
      ChangelogInfo changelogInfo =
          new ChangelogInfo(replicationFactor, kafkaConfig.getChangelogKafkaProperties(storeName));
      LOG.info(String.format("Creating topic meta information for topic: %s with replication factor: %s", topicName,
          replicationFactor));
      changelogTopicMetaInformation.put(topicName, changelogInfo);
    }

    // special flag to allow/enforce deleting of committed messages
    SystemConfig systemConfig = new SystemConfig(config);
    this.deleteCommittedMessages = systemConfig.deleteCommittedMessages(systemName);

    intermediateStreamProperties = getIntermediateStreamProperties(config);

    LOG.info(String.format("Created KafkaSystemAdmin for system %s", systemName));
  }

  @Override
  public void start() {
    // Plese note. There is slight inconsistency in the use of this class.
    // Some of the functionality of this class may actually be used BEFORE start() is called.
    // The SamzaContainer gets metadata (using this class) in SamzaContainer.apply,
    // but this "start" actually gets called in SamzaContainer.run.
    // review this usage (SAMZA-1888)

    // Throw exception if start is called after stop
    if (stopped.get()) {
      throw new IllegalStateException("SamzaKafkaAdmin.start() is called after stop()");
    }
  }

  @Override
  public void stop() {
    if (stopped.compareAndSet(false, true)) {
      try {
        metadataConsumer.close();
      } catch (Exception e) {
        LOG.warn("metadataConsumer.close for system " + systemName + " failed with exception.", e);
      }
    }

    if (adminClient != null) {
      adminClient.close();
    }
  }

  /**
   * Note! This method does not populate SystemStreamMetadata for each stream with real data.
   * Thus, this method should ONLY be used to get number of partitions for each stream.
   * It will throw NotImplementedException if anyone tries to access the actual metadata.
   * @param streamNames set of streams for which get the partitions counts
   * @param cacheTTL cache TTL if caching the data
   * @return a map, keyed on stream names. Number of partitions in SystemStreamMetadata is the output of this method.
   */
  @Override
  public Map<String, SystemStreamMetadata> getSystemStreamPartitionCounts(Set<String> streamNames, long cacheTTL) {
    // This optimization omits actual metadata for performance. Instead, we inject a dummy for all partitions.
    final SystemStreamMetadata.SystemStreamPartitionMetadata dummySspm =
        new SystemStreamMetadata.SystemStreamPartitionMetadata(null, null, null) {
          String msg =
              "getSystemStreamPartitionCounts does not populate SystemStreaMetadata info. Only number of partitions";

          @Override
          public String getOldestOffset() {
            throw new NotImplementedException(msg);
          }

          @Override
          public String getNewestOffset() {
            throw new NotImplementedException(msg);
          }

          @Override
          public String getUpcomingOffset() {
            throw new NotImplementedException(msg);
          }
        };

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

              partitionInfos.forEach(partitionInfo -> partitionMetadata.put(new Partition(partitionInfo.partition()), dummySspm));

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

    LOG.info("SystemStream partition counts for system {}: {}", systemName, result);
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

    OffsetsMaps topicPartitionsMetadata = fetchTopicPartitionsMetadata(topicPartitions);

    Map<SystemStreamPartition, SystemStreamMetadata.SystemStreamPartitionMetadata> sspToSSPMetadata = new HashMap<>();
    for (SystemStreamPartition ssp : ssps) {
      String oldestOffset = topicPartitionsMetadata.getOldestOffsets().get(ssp);
      String newestOffset = topicPartitionsMetadata.getNewestOffsets().get(ssp);
      String upcomingOffset = topicPartitionsMetadata.getUpcomingOffsets().get(ssp);

      sspToSSPMetadata.put(ssp,
          new SystemStreamMetadata.SystemStreamPartitionMetadata(oldestOffset, newestOffset, upcomingOffset));
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

    return retryBackoff.run(fetchMetadataOperation, onExceptionRetryOperation).getOrElse(fallbackOperation);
  }

  /**
   * Convert TopicPartition to SystemStreamPartition
   * @param topicPartition the topic partition to be created
   * @return an instance of SystemStreamPartition
   */
  private SystemStreamPartition toSystemStreamPartition(TopicPartition topicPartition) {
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

    oldestOffsetsWithLong.forEach((topicPartition, offset) -> oldestOffsets.put(toSystemStreamPartition(topicPartition), String.valueOf(offset)));

    upcomingOffsetsWithLong.forEach((topicPartition, offset) -> {
      upcomingOffsets.put(toSystemStreamPartition(topicPartition), String.valueOf(offset));

      // Kafka's beginning Offset corresponds to the offset for the oldest message.
      // Kafka's end offset corresponds to the offset for the upcoming message, and it is the newest offset + 1.
      // When upcoming offset is <=0, the topic appears empty, we put oldest offset 0 and the newest offset null.
      // When upcoming offset is >0, we subtract the upcoming offset by one for the newest offset.
      // For normal case, the newest offset will correspond to the offset of the newest message in the stream;
      // But for the big message, it is not the case. Seeking on the newest offset gives nothing for the newest big message.
      // For now, we keep it as is for newest offsets the same as historical metadata structure.
      if (offset <= 0) {
        LOG.warn(
            "Empty Kafka topic partition {} with upcoming offset {}. Skipping newest offset and setting oldest offset to 0 to consume from beginning",
            topicPartition, offset);
        oldestOffsets.put(toSystemStreamPartition(topicPartition), "0");
      } else {
        newestOffsets.put(toSystemStreamPartition(topicPartition), String.valueOf(offset - 1));
      }
    });
    return new OffsetsMaps(oldestOffsets, newestOffsets, upcomingOffsets);
  }

  /**
   * Fetch SystemStreamMetadata for each topic with the consumer
   * @param topics set of topics to get metadata info for
   * @return map of topic to SystemStreamMetadata
   */
  private Map<String, SystemStreamMetadata> fetchSystemStreamMetadata(Set<String> topics) {
    Map<SystemStreamPartition, String> allOldestOffsets = new HashMap<>();
    Map<SystemStreamPartition, String> allNewestOffsets = new HashMap<>();
    Map<SystemStreamPartition, String> allUpcomingOffsets = new HashMap<>();

    LOG.info("Fetching SystemStreamMetadata for topics {} on system {}", topics, systemName);

    topics.forEach(topic -> {
      List<PartitionInfo> partitionInfos = metadataConsumer.partitionsFor(topic);

      if (partitionInfos == null) {
        String msg = String.format("Partition info not(yet?) available for system %s topic %s", systemName, topic);
        throw new SamzaException(msg);
      }

      List<TopicPartition> topicPartitions = partitionInfos.stream()
          .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
          .collect(Collectors.toList());

      OffsetsMaps offsetsForTopic = fetchTopicPartitionsMetadata(topicPartitions);
      allOldestOffsets.putAll(offsetsForTopic.getOldestOffsets());
      allNewestOffsets.putAll(offsetsForTopic.getNewestOffsets());
      allUpcomingOffsets.putAll(offsetsForTopic.getUpcomingOffsets());
    });

    return assembleMetadata(allOldestOffsets, allNewestOffsets, allUpcomingOffsets);
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
    final String REPL_FACTOR = "replication.factor";

    KafkaStreamSpec kSpec = toKafkaSpec(streamSpec);
    String topicName = kSpec.getPhysicalName();

    // create topic.
    NewTopic newTopic = new NewTopic(topicName, kSpec.getPartitionCount(), (short) kSpec.getReplicationFactor());

    // specify the configs
    Map<String, String> streamConfig = new HashMap<>(streamSpec.getConfig());
    // HACK - replication.factor is invalid config for AdminClient.createTopics
    if (streamConfig.containsKey(REPL_FACTOR)) {
      String repl = streamConfig.get(REPL_FACTOR);
      LOG.warn("Configuration {}={} for topic={} is invalid. Using kSpec repl factor {}",
          REPL_FACTOR, repl, kSpec.getPhysicalName(), kSpec.getReplicationFactor());
      streamConfig.remove(REPL_FACTOR);
    }
    newTopic.configs(new MapConfig(streamConfig));
    CreateTopicsResult result = adminClient.createTopics(ImmutableSet.of(newTopic));
    try {
      result.all().get(KAFKA_ADMIN_OPS_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      if (e instanceof TopicExistsException || e.getCause() instanceof TopicExistsException) {
        LOG.info("Topic {} already exists.", topicName);
        return false;
      }

      throw new SamzaException(String.format("Creation of topic %s failed.", topicName), e);
    }
    LOG.info("Successfully created topic {}", topicName);
    DescribeTopicsResult desc = adminClient.describeTopics(ImmutableSet.of(topicName));
    try {
      TopicDescription td = desc.all().get(KAFKA_ADMIN_OPS_TIMEOUT_MS, TimeUnit.MILLISECONDS).get(topicName);
      LOG.info("Topic {} created with {}", topicName, td);
      return true;
    } catch (Exception e) {
      LOG.error("'Describe after create' failed for topic " + topicName, e);
      return false;
    }
  }

  @Override
  public boolean clearStream(StreamSpec streamSpec) {
    LOG.info("Creating Kafka topic: {} on system: {}", streamSpec.getPhysicalName(), streamSpec.getSystemName());

    String topicName = streamSpec.getPhysicalName();

    try {
      DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(ImmutableSet.of(topicName));
      deleteTopicsResult.all().get(KAFKA_ADMIN_OPS_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      LOG.error("Failed to delete topic {} with exception {}.", topicName, e);
      return false;
    }

    return true;
  }

  /**
   * Converts a StreamSpec into a KafkaStreamSpec. Special handling for coordinator and changelog stream.
   * @param spec a StreamSpec object
   * @return KafkaStreamSpec object
   */
  public KafkaStreamSpec toKafkaSpec(StreamSpec spec) {
    KafkaStreamSpec kafkaSpec;
    if (spec.isChangeLogStream()) {
      String topicName = spec.getPhysicalName();
      ChangelogInfo topicMeta = changelogTopicMetaInformation.get(topicName);
      if (topicMeta == null) {
        throw new StreamValidationException("Unable to find topic information for topic " + topicName);
      }

      kafkaSpec = new KafkaStreamSpec(spec.getId(), topicName, systemName, spec.getPartitionCount(),
          topicMeta.getReplicationFactor(), topicMeta.getKafkaProperties());
    } else if (spec.isCoordinatorStream()) {
      kafkaSpec =
          new KafkaStreamSpec(spec.getId(), spec.getPhysicalName(), systemName, 1, coordinatorStreamReplicationFactor,
              coordinatorStreamProperties);
    } else if (intermediateStreamProperties.containsKey(spec.getId())) {
      kafkaSpec = KafkaStreamSpec.fromSpec(spec);
      Properties properties = kafkaSpec.getProperties();
      properties.putAll(intermediateStreamProperties.get(spec.getId()));
      kafkaSpec = kafkaSpec.copyWithProperties(properties);
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

  // get partition info for topic
  Map<String, List<PartitionInfo>> getTopicMetadata(Set<String> topics) {
    Map<String, List<PartitionInfo>> streamToPartitionsInfo = new HashMap<>();
    List<PartitionInfo> partitionInfoList;
    for (String topic : topics) {
      partitionInfoList = metadataConsumer.partitionsFor(topic);
      streamToPartitionsInfo.put(topic, partitionInfoList);
    }

    return streamToPartitionsInfo;
  }

  /**
   * Delete records up to (and including) the provided ssp offsets for
   * all system stream partitions specified in the map.
   * This only works with Kafka cluster 0.11 or later. Otherwise it's a no-op.
   * @param offsets specifies up to what offsets the messages should be deleted
   */
  @Override
  public void deleteMessages(Map<SystemStreamPartition, String> offsets) {
    if (deleteCommittedMessages) {
      Map<TopicPartition, RecordsToDelete> recordsToDelete = offsets.entrySet()
          .stream()
          .collect(Collectors.toMap(entry ->
              new TopicPartition(entry.getKey().getStream(), entry.getKey().getPartition().getPartitionId()),
              entry -> RecordsToDelete.beforeOffset(Long.parseLong(entry.getValue()) + 1)));

      adminClient.deleteRecords(recordsToDelete).all().whenComplete((ignored, exception) -> {
        if (exception != null) {
          LOG.error("Delete message failed for SSPs " + offsets.keySet() + " due to", exception);
        }
      });

      deleteMessageCalled = true;
    }
  }

  protected Properties createAdminClientProperties() {
    // populate brokerList from either consumer or producer configs
    Properties props = new Properties();
    // included SSL settings if needed

    props.putAll(config.subset(String.format("systems.%s.consumer.", systemName), true));

    //validate brokerList
    String brokerList = config.get(
        String.format(KafkaConfig.CONSUMER_CONFIGS_CONFIG_KEY(), systemName, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    if (brokerList == null) {
      brokerList = config.get(String.format(KafkaConfig.PRODUCER_CONFIGS_CONFIG_KEY(), systemName,
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }
    if (brokerList == null) {
      throw new SamzaException(
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG + " is required for systemAdmin for system " + systemName);
    }
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

    return props;
  }

  @Override
  public Set<SystemStream> getAllSystemStreams() {
    return ((Set<String>) this.metadataConsumer.listTopics().keySet()).stream()
        .map(x -> new SystemStream(systemName, x))
        .collect(Collectors.toSet());
  }

  /**
   * A helper method that takes oldest, newest, and upcoming offsets for each
   * system stream partition, and creates a single map from stream name to
   * SystemStreamMetadata.
   *
   * @param newestOffsets map of SSP to newest offset
   * @param oldestOffsets map of SSP to oldest offset
   * @param upcomingOffsets map of SSP to upcoming offset
   * @return a {@link Map} from {@code system} to {@link SystemStreamMetadata}
   */
  @VisibleForTesting
  static Map<String, SystemStreamMetadata> assembleMetadata(Map<SystemStreamPartition, String> oldestOffsets,
      Map<SystemStreamPartition, String> newestOffsets, Map<SystemStreamPartition, String> upcomingOffsets) {
    HashSet<SystemStreamPartition> allSSPs = new HashSet<>();
    allSSPs.addAll(oldestOffsets.keySet());
    allSSPs.addAll(newestOffsets.keySet());
    allSSPs.addAll(upcomingOffsets.keySet());

    Map<String, SystemStreamMetadata> assembledMetadata = allSSPs.stream()
        .collect(Collectors.groupingBy(SystemStreamPartition::getStream))
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
          Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> partitionMetadata =
              entry.getValue()
                  .stream()
                  .collect(Collectors.toMap(SystemStreamPartition::getPartition, ssp ->
                      new SystemStreamMetadata.SystemStreamPartitionMetadata(
                          oldestOffsets.getOrDefault(ssp, null),
                          newestOffsets.getOrDefault(ssp, null),
                          upcomingOffsets.get(ssp))));
          return new SystemStreamMetadata(entry.getKey(), partitionMetadata);
        }));

    return assembledMetadata;
  }

  /**
   * Fetch stream properties for all intermediate streams.
   *
   * @param config kafka system config
   * @return a {@link Map} from {@code streamId} to stream {@link Properties}
   */
  @VisibleForTesting
  static Map<String, Properties> getIntermediateStreamProperties(Config config) {
    Map<String, Properties> intermedidateStreamProperties = Collections.emptyMap();
    ApplicationConfig appConfig = new ApplicationConfig(config);

    if (appConfig.getAppMode() == ApplicationConfig.ApplicationMode.BATCH) {
      StreamConfig streamConfig = new StreamConfig(config);
      intermedidateStreamProperties = JavaConverters.asJavaCollectionConverter(streamConfig.getStreamIds())
          .asJavaCollection()
          .stream()
          .filter(streamConfig::getIsIntermediateStream)
          .collect(Collectors.toMap(Function.identity(), streamId -> {
            Properties properties = new Properties();
            properties.putAll(streamConfig.getStreamProperties(streamId));
            properties.putIfAbsent(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(KafkaConfig.DEFAULT_RETENTION_MS_FOR_BATCH()));
            return properties;
          }));
    }

    return intermedidateStreamProperties;
  }

  private Properties getCoordinatorStreamProperties(KafkaConfig config) {
    Properties coordinatorStreamProperties = new Properties();
    coordinatorStreamProperties.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
    coordinatorStreamProperties.put(TopicConfig.SEGMENT_BYTES_CONFIG, config.getCoordinatorSegmentBytes());

    return coordinatorStreamProperties;
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

  /**
   * A helper class for represent changelog related information.
   */
  private static class ChangelogInfo {
    final int replicationFactor;
    final Properties kafkaProperties;

    /**
     * @param replicationFactor The number of replicas for the changelog stream
     * @param kafkaProperties The kafka specific properties that need to be used for changelog stream creation
     */
    ChangelogInfo(int replicationFactor, Properties kafkaProperties) {
      this.replicationFactor = replicationFactor;
      this.kafkaProperties = kafkaProperties;
    }

    public int getReplicationFactor() {
      return replicationFactor;
    }

    public Properties getKafkaProperties() {
      return kafkaProperties;
    }
  }
}
