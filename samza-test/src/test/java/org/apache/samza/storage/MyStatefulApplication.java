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

package org.apache.samza.storage;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.context.Context;
import org.apache.samza.operators.KV;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.storage.kv.descriptors.RocksDbTableDescriptor;
import org.apache.samza.storage.kv.inmemory.descriptors.InMemoryTableDescriptor;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.task.TaskCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateful TaskApplication used for testing task store backup and restore behaviour.
 * {@link #resetTestState()} should be invoked in @Before class of the test using this class.
 *
 * Input Message format:
 * "num" -> put (key = num, value = num) and flush
 * "-num" -> delete (key = num) and flush
 * ":msg" -> act on msg (including flush) but no commit (may be num, shutdown or crash_once)
 * "shutdown" -> always shutdown the job
 * "crash_once" -> shut down the job the first time but ignore on subsequent run
 */
public class MyStatefulApplication implements TaskApplication {

  public static final Logger LOG = LoggerFactory.getLogger(MyStatefulApplication.class);

  private static Map<String, List<String>> initialStoreContents = new HashMap<>();
  private static Map<String, List<String>> initialInMemoryStoreContents = new HashMap<>();
  private static Map<String, List<String>> initialSideInputStoreContents = new HashMap<>();
  private static boolean crashedOnce = false;

  private final String inputSystem;
  private final String inputTopic;
  private final Set<String> storeNames;
  private final Map<String, String> storeNamesToChangelog;
  private final Set<String> inMemoryStoreNames;
  private final Map<String, String> inMemoryStoreNamesToChangelog;
  private final Optional<String> sideInputStoreName;
  private final Optional<String> sideInputTopic;
  private final Optional<SideInputsProcessor> sideInputProcessor;

  public MyStatefulApplication(String inputSystem, String inputTopic,
      Set<String> storeNames, Map<String, String> storeNamesToChangelog,
      Set<String> inMemoryStoreNames, Map<String, String> inMemoryStoreNamesToChangelog,
      Optional<String> sideInputStoreName, Optional<String> sideInputTopic,
      Optional<SideInputsProcessor> sideInputProcessor) {
    this.inputSystem = inputSystem;
    this.inputTopic = inputTopic;
    this.storeNames = storeNames;
    this.storeNamesToChangelog = storeNamesToChangelog;
    this.inMemoryStoreNames = inMemoryStoreNames;
    this.inMemoryStoreNamesToChangelog = inMemoryStoreNamesToChangelog;
    this.sideInputStoreName = sideInputStoreName;
    this.sideInputTopic = sideInputTopic;
    this.sideInputProcessor = sideInputProcessor;
  }

  @Override
  public void describe(TaskApplicationDescriptor appDescriptor) {
    KafkaSystemDescriptor ksd = new KafkaSystemDescriptor(inputSystem);
    KVSerde<String, String> serde = KVSerde.of(new StringSerde(), new StringSerde());

    KafkaInputDescriptor<KV<String, String>> isd = ksd.getInputDescriptor(inputTopic, serde);

    TaskApplicationDescriptor desc = appDescriptor
        .withInputStream(isd)
        .withTaskFactory((StreamTaskFactory) () -> new MyTask(storeNames, inMemoryStoreNames, sideInputStoreName));

    inMemoryStoreNames.forEach(storeName -> {
      InMemoryTableDescriptor<String, String> imtd;
      if (inMemoryStoreNamesToChangelog.containsKey(storeName)) {
        imtd = new InMemoryTableDescriptor<>(storeName, serde)
            .withChangelogStream(inMemoryStoreNamesToChangelog.get(storeName));
      } else {
        imtd = new InMemoryTableDescriptor<>(storeName, serde);
      }

      desc.withTable(imtd);
    });

    storeNames.forEach(storeName -> {
      RocksDbTableDescriptor<String, String> td;
      if (storeNamesToChangelog.containsKey(storeName)) {
        String changelogTopic = storeNamesToChangelog.get(storeName);
        td = new RocksDbTableDescriptor<>(storeName, serde)
            .withChangelogStream(changelogTopic)
            .withChangelogReplicationFactor(1);
      } else {
        td = new RocksDbTableDescriptor<>(storeName, serde);
      }
      desc.withTable(td);
    });

    if (sideInputStoreName.isPresent()) {
      RocksDbTableDescriptor<String, String> sideInputStoreTd =
          new RocksDbTableDescriptor<>(sideInputStoreName.get(), serde)
              .withSideInputs(ImmutableList.of(sideInputTopic.get()))
              .withSideInputsProcessor(sideInputProcessor.get());
      desc.withTable(sideInputStoreTd);
    }
  }

  public static void resetTestState() {
    initialStoreContents = new HashMap<>();
    initialInMemoryStoreContents = new HashMap<>();
    initialSideInputStoreContents = new HashMap<>();
    crashedOnce = false;
  }

  public static Map<String, List<String>> getInitialStoreContents() {
    return initialStoreContents;
  }

  public static Map<String, List<String>> getInitialInMemoryStoreContents() {
    return initialInMemoryStoreContents;
  }

  public static Map<String, List<String>> getInitialSideInputStoreContents() {
    return initialSideInputStoreContents;
  }

  static class MyTask implements StreamTask, InitableTask {
    private final Set<KeyValueStore<String, String>> stores = new HashSet<>();
    private final Set<String> storeNames;
    private final Set<String> inMemoryStoreNames;
    private final Optional<String> sideInputStoreName;

    MyTask(Set<String> storeNames, Set<String> inMemoryStoreNames, Optional<String> sideInputStoreName) {
      this.storeNames = storeNames;
      this.inMemoryStoreNames = inMemoryStoreNames;
      this.sideInputStoreName = sideInputStoreName;
    }

    @Override
    public void init(Context context) {
      storeNames.forEach(storeName -> {
        KeyValueStore<String, String> store = (KeyValueStore<String, String>) context.getTaskContext().getStore(storeName);
        LOG.debug("For storename:{} received: {}", storeName, store);
        stores.add(store); // any input messages will be written to all 'stores'
        KeyValueIterator<String, String> storeEntries = store.all();
        List<String> storeInitialContents = new ArrayList<>();
        while (storeEntries.hasNext()) {
          LOG.debug("INIT {} Store content. StoreInitialContent: {}", storeName, storeInitialContents);
          storeInitialContents.add(storeEntries.next().getValue());
        }
        initialStoreContents.put(storeName, storeInitialContents);
        storeEntries.close();
      });

      inMemoryStoreNames.forEach(storeName -> {
        KeyValueStore<String, String> store =
            (KeyValueStore<String, String>) context.getTaskContext().getStore(storeName);
        LOG.debug("For storename:{} received: {}", storeName, store);
        stores.add(store); // any input messages will be written to all 'stores'.
        KeyValueIterator<String, String> storeEntries = store.all();
        List<String> storeInitialContents = new ArrayList<>();
        LOG.debug("INIT InMemory Store content:{} ", storeName);
        while (storeEntries.hasNext()) {
          storeInitialContents.add(storeEntries.next().getValue());
          LOG.debug("INIT InMemory Store content. StoreInitialContent: {}", storeInitialContents);
        }
        initialInMemoryStoreContents.put(storeName, storeInitialContents);
        storeEntries.close();
      });

      if (sideInputStoreName.isPresent()) {
        KeyValueStore<String, String> sideInputStore =
            (KeyValueStore<String, String>) context.getTaskContext().getStore(sideInputStoreName.get());
        LOG.debug("For storename:{} received: {}", sideInputStoreName, sideInputStore);
        KeyValueIterator<String, String> sideInputStoreEntries = sideInputStore.all();
        List<String> sideInputStoreInitialContents = new ArrayList<>();
        while (sideInputStoreEntries.hasNext()) {
          sideInputStoreInitialContents.add(sideInputStoreEntries.next().getValue());
        }
        initialSideInputStoreContents.put(sideInputStoreName.get(), sideInputStoreInitialContents);
        sideInputStoreEntries.close();
      }
    }

    @Override
    public void process(IncomingMessageEnvelope envelope,
        MessageCollector collector, TaskCoordinator coordinator) {
      String key = (String) envelope.getKey();
      LOG.info("Received key: {}", key);

      if (key.endsWith("crash_once")) {  // endsWith allows :crash_once and crash_once
        if (!crashedOnce) {
          LOG.debug("Process in my task: CrashOnce received.");
          crashedOnce = true;
          coordinator.shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
        } else {
          return;
        }
      } else if (key.endsWith("shutdown")) {
        LOG.debug("Process in my task: Shutdown received.");
        coordinator.shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
      } else if (key.startsWith("-")) {
        stores.forEach(store -> {
          LOG.debug("Process in my task: - received. Deleting: {} from {}", key, store);
          store.delete(key.substring(1));
        });
      } else if (key.startsWith(":")) {
        // write the message and flush, but don't invoke commit later
        String msg = key.substring(1);
        stores.forEach(store -> {
          LOG.debug("Process in my task: ':' received. Put {} in {}", msg, store);
          store.put(msg, msg);
        });
      } else {
        stores.forEach(store -> {
          LOG.debug("Process in my task: Adding key to store. Put {} in {}", key, store);
          store.put(key, key);
        });
      }
      LOG.debug("Process in my task: Flush received.");
      stores.forEach(KeyValueStore::flush);

      if (!key.startsWith(":")) {
        LOG.debug("Process in my task: ':' not received. Calling commit: {}", TaskCoordinator.RequestScope.CURRENT_TASK);
        coordinator.commit(TaskCoordinator.RequestScope.CURRENT_TASK);
      }
    }
  }
}
