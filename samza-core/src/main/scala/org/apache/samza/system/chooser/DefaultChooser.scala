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

package org.apache.samza.system.chooser

import org.apache.samza.SamzaException
import org.apache.samza.config.{Config, DefaultChooserConfig, TaskConfigJava}
import org.apache.samza.metrics.{MetricsRegistry, MetricsRegistryMap}
import org.apache.samza.system._
import org.apache.samza.util.Logging
import java.util.HashMap
import scala.collection.JavaConverters._


object DefaultChooser extends Logging {
  def apply(inputStreamMetadata: Map[SystemStream, SystemStreamMetadata],
            chooserFactory: MessageChooserFactory,
            config: Config,
            registry: MetricsRegistry,
            systemAdmins: SystemAdmins) = {
    val chooserConfig = new DefaultChooserConfig(config)
    val batchSize = if (chooserConfig.getChooserBatchSize > 0) Some(chooserConfig.getChooserBatchSize) else None

    debug("Got batch size: %s" format batchSize)

    // Normal streams default to priority 0.
    val defaultPrioritizedStreams = new TaskConfigJava(config)
      .getAllInputStreams.asScala
      .map((_, 0))
      .toMap

    debug("Got default priority streams: %s" format defaultPrioritizedStreams)

    // Bootstrap streams default to Int.MaxValue priority.
    val prioritizedBootstrapStreams = chooserConfig
      .getBootstrapStreams.asScala
      .map((_, Int.MaxValue))
      .toMap

    debug("Got bootstrap priority streams: %s" format prioritizedBootstrapStreams)

    // Explicitly prioritized streams are set to whatever they were configured to.
    val prioritizedStreams = chooserConfig.getPriorityStreams.asScala.mapValues(_.asInstanceOf[Int])

    debug("Got prioritized streams: %s" format prioritizedStreams)

    // Only wire in what we need.
    val useBootstrapping = prioritizedBootstrapStreams.size > 0
    val usePriority = useBootstrapping || prioritizedStreams.size > 0
    val bootstrapStreamMetadata = inputStreamMetadata
      .filterKeys(prioritizedBootstrapStreams.contains(_))

    debug("Got bootstrap stream metadata: %s" format bootstrapStreamMetadata)

    val priorities = if (usePriority) {
      // Ordering is important here. Overrides Int.MaxValue default for
      // bootstrap streams with explicitly configured values, in cases where
      // users have defined a bootstrap stream's priority in config.
      defaultPrioritizedStreams ++ prioritizedBootstrapStreams ++ prioritizedStreams
    } else {
      Map[SystemStream, Int]()
    }

    debug("Got fully prioritized stream list: %s" format priorities)

    val prioritizedChoosers = priorities
      .values
      .toSet
      .map((_: Int, chooserFactory.getChooser(config, registry)))
      .toMap

    new DefaultChooser(
      chooserFactory.getChooser(config, registry),
      batchSize,
      priorities,
      prioritizedChoosers,
      bootstrapStreamMetadata,
      registry,
      systemAdmins)
  }
}

/**
 * DefaultChooser adds additional functionality to an existing MessageChooser.
 *
 * The following behaviors are currently supported:
 *
 * 1. Batching.
 * 2. Prioritized streams.
 * 3. Bootstrapping.
 *
 * By default, this chooser will not do any of this. It will simply default to
 * a RoundRobinChooser.
 *
 * To activate batching, you must define:
 *
 *   task.consumer.batch.size
 *
 * To define a priority for a stream, you must define:
 *
 *   systems.<system>.streams.<stream>.samza.priority
 *
 * To declare a bootstrap stream, you must define:
 *
 *   systems.<system>.streams.<stream>.samza.bootstrap
 *
 * When batching is activated, the DefaultChooserFactory will allow the
 * initial strategy to be executed once (by default, this is RoundRobin). It
 * will then keep picking the SystemStreamPartition that the RoundRobin
 * chooser selected, up to the batch size, provided that there are messages
 * available for this SystemStreamPartition. If the batch size is reached, or
 * there are no messages available, the RoundRobinChooser will be executed
 * again, and the batching process will repeat itself.
 *
 * When a stream is defined with a priority, it is preferred over all lower
 * priority streams in cases where there are messages available from both
 * streams. If two envelopes exist for two SystemStreamPartitions that have
 * the same priority, the default strategy is used to determine which envelope
 * to use (RoundRobinChooser, by default). If a stream doesn't have a
 * configured priority, its priority is 0. Higher priority streams are
 * preferred over lower priority streams.
 *
 * When a stream is defined as a bootstrap stream, it is prioritized with a
 * default priority of Int.MaxValue. This priority can be overridden using the
 * same priority configuration defined above (task.chooser.priorites.*). The
 * DefaultChooserFactory guarantees that the wrapped MessageChooser will have
 * at least one envelope from each bootstrap stream whenever the wrapped
 * MessageChooser must make a decision about which envelope to process next.
 * If a stream is defined as a bootstrap stream, and is prioritized higher
 * than all other streams, it means that all messages in the stream will be
 * processed (up to head) before any other messages are processed. Once all of
 * a bootstrap stream's partitions catch up to head, the stream is marked as
 * fully bootstrapped, and it is then treated like a normal prioritized stream.
 *
 * Valid configurations include:
 *
 *   task.consumer.batch.size=100
 *
 * This configuration will just batch up to 100 messages from each
 * SystemStreamPartition. It will use a RoundRobinChooser whenever it needs to
 * find the next SystemStreamPartition to batch.
 *
 *   systems.kafka.streams.mystream.samza.priority=1
 *
 * This configuration will prefer messages from kafka.mystream over any other
 * input streams (since other input streams will default to priority 0).
 *
 *   systems.kafka.streams.profiles.samza.bootstrap=true
 *
 * This configuration will process all messages from kafka.profiles up to the
 * current head of the profiles stream before any other messages are processed.
 * From then on, the profiles stream will be preferred over any other stream in
 * cases where incoming envelopes are ready to be processed from it.
 *
 *   task.consumer.batch.size=100
 *   systems.kafka.streams.profiles.samza.bootstrap=true
 *   systems.kafka.streams.mystream.samza.priority=1
 *
 * This configuration will read all messages from kafka.profiles from the last
 * checkpointed offset, up to head. It will then prefer messages from profiles
 * over mystream, and prefer messages from mystream over any other stream. In
 * cases where there is more than one envelope available with the same priority
 * (e.g. two envelopes from different partitions in the profiles stream),
 * RoundRobinChooser will be used to break the tie. Once the tie is broken, up
 * to 100 messages will be read from the envelope's SystemStreamPartition,
 * before RoundRobinChooser is consulted again to break the next tie.
 *
 *   systems.kafka.streams.profiles.samza.bootstrap=true
 *   systems.kafka.streams.profiles.samza.reset.offset=true
 *
 * This configuration will bootstrap the profiles stream the same way as the
 * last example, except that it will always start from offset zero, which means
 * that it will always read all messages in the topic from oldest to newest.
 */
class DefaultChooser(
  /**
   * The wrapped chooser serves two purposes. In cases where bootstrapping or
   * prioritization is enabled, wrapped chooser serves as the default for
   * envelopes that have no priority defined.
   *
   * When prioritization and bootstrapping are not enabled, but batching is,
   * wrapped chooser is used as the strategy to determine which
   * SystemStreamPartition to batch next.
   *
   * When nothing is enabled, DefaultChooser just acts as a pass through for
   * the wrapped chooser.
   */
  DefaultChooser: MessageChooser = new RoundRobinChooser,

  /**
   * If defined, enables batching, and defines a max message size for a given
   * batch. Once the batch size is exceeded (at least batchSize messages have
   * been processed from a single system stream), the wrapped chooser is used
   * to determine the next system stream to process.
   */
  batchSize: Option[Int] = None,

  /**
   * Defines a mapping from SystemStream to a priority tier. Envelopes from
   * higher priority SystemStreams are processed before envelopes from lower
   * priority SystemStreams.
   *
   * If multiple envelopes exist within a single tier, the prioritized chooser
   * (defined below) is used to break the tie.
   *
   * If this map is empty, prioritization will not happen.
   */
  prioritizedStreams: Map[SystemStream, Int] = Map(),

  /**
   * Defines the tie breaking strategy to be used at each tier of the priority
   * chooser. This chooser is used to break the tie when more than one envelope
   * exists with the same priority.
   */
  prioritizedChoosers: Map[Int, MessageChooser] = Map(),

  /**
   * Defines a mapping from SystemStreamPartition to the metadata associated
   * with it. This is useful for checking whether we've caught up with a
   * bootstrap stream, since the metadata contains offset information about
   * the stream partition. Bootstrap streams are marked as "behind" until all
   * SSPs for the SystemStream have been read. Once the bootstrap stream has
   * been "caught up" it is removed from the bootstrap set, and treated as a
   * normal stream.
   *
   * If this map is empty, no streams will be treated as bootstrap streams.
   *
   * Using bootstrap streams automatically enables stream prioritization.
   * Bootstrap streams default to a priority of Int.MaxValue.
   */
  bootstrapStreamMetadata: Map[SystemStream, SystemStreamMetadata] = Map(),

  /**
   * Metrics registry to be used when wiring up wrapped choosers.
   */
  registry: MetricsRegistry = new MetricsRegistryMap,

  /**
   * Defines a mapping from SystemStream name to SystemAdmin.
   * This is useful for determining if a bootstrap SystemStream is caught up.
   */
  systemAdmins: SystemAdmins = new SystemAdmins(new HashMap[String, SystemAdmin])) extends MessageChooser with Logging {

  val chooser = {
    val useBatching = batchSize.isDefined
    val useBootstrapping = bootstrapStreamMetadata.size > 0
    val usePriority = useBootstrapping || prioritizedStreams.size > 0

    info("Building default chooser with: useBatching=%s, useBootstrapping=%s, usePriority=%s" format (useBatching, useBootstrapping, usePriority))

    val maybePrioritized = if (usePriority) {
      new TieredPriorityChooser(prioritizedStreams, prioritizedChoosers, DefaultChooser)
    } else if (DefaultChooser == null) {
      // Null wrapped chooser without a priority chooser is not allowed
      // because DefaultChooser needs an underlying message chooser.
      throw new SamzaException("A null chooser was given to the DefaultChooser. This is not allowed unless you are using prioritized/bootstrap streams, which you're not.")
    } else {
      DefaultChooser
    }

    val maybeBatching = if (useBatching) {
      new BatchingChooser(maybePrioritized, batchSize.get, new BatchingChooserMetrics(registry))
    } else {
      maybePrioritized
    }

    if (useBootstrapping) {
      new BootstrappingChooser(maybeBatching, bootstrapStreamMetadata, new BootstrappingChooserMetrics(registry), systemAdmins)
    } else {
      maybeBatching
    }
  }

  def update(envelope: IncomingMessageEnvelope) {
    chooser.update(envelope)
  }

  def choose = chooser.choose

  def start = chooser.start

  def stop = chooser.stop

  def register(systemStreamPartition: SystemStreamPartition, offset: String) = chooser.register(systemStreamPartition, offset)
}
