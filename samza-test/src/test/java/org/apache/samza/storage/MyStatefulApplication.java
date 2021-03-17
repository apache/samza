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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
  private static boolean crashedOnce = false;
  private final String inputSystem;
  private final String inputTopic;
  private final Map<String, String> storeToChangelog;

  public MyStatefulApplication(String inputSystem, String inputTopic, Map<String, String> storeToChangelog) {
    this.inputSystem = inputSystem;
    this.inputTopic = inputTopic;
    this.storeToChangelog = storeToChangelog;
  }

  @Override
  public void describe(TaskApplicationDescriptor appDescriptor) {
    KafkaSystemDescriptor ksd = new KafkaSystemDescriptor(inputSystem);
    KVSerde<String, String> serde = KVSerde.of(new StringSerde(), new StringSerde());

    KafkaInputDescriptor<KV<String, String>> isd = ksd.getInputDescriptor(inputTopic, serde);

    TaskApplicationDescriptor desc = appDescriptor
        .withInputStream(isd)
        .withTaskFactory((StreamTaskFactory) () -> new MyTask(storeToChangelog.keySet()));

    storeToChangelog.forEach((storeName, changelogTopic) -> {
      RocksDbTableDescriptor<String, String> td = new RocksDbTableDescriptor<>(storeName, serde)
          .withChangelogStream(changelogTopic)
          .withChangelogReplicationFactor(1);
      desc.withTable(td);
    });
  }

  public static void resetTestState() {
    initialStoreContents = new HashMap<>();
    crashedOnce = false;
  }

  public static Map<String, List<String>> getInitialStoreContents() {
    return initialStoreContents;
  }

  static class MyTask implements StreamTask, InitableTask {
    private final Set<KeyValueStore<String, String>> stores = new HashSet<>();
    private final Set<String> storeNames;

    MyTask(Set<String> storeNames) {
      this.storeNames = storeNames;
    }

    @Override
    public void init(Context context) {
      storeNames.forEach(storeName -> {
        KeyValueStore<String, String> store = (KeyValueStore<String, String>) context.getTaskContext().getStore(storeName);
        stores.add(store);
        KeyValueIterator<String, String> storeEntries = store.all();
        List<String> storeInitialChangelog = new ArrayList<>();
        while (storeEntries.hasNext()) {
          storeInitialChangelog.add(storeEntries.next().getValue());
        }
        initialStoreContents.put(storeName, storeInitialChangelog);
        storeEntries.close();
      });
    }

    @Override
    public void process(IncomingMessageEnvelope envelope,
        MessageCollector collector, TaskCoordinator coordinator) {
      String key = (String) envelope.getKey();
      LOG.info("Received key: {}", key);

      if (key.endsWith("crash_once")) {  // endsWith allows :crash_once and crash_once
        if (!crashedOnce) {
          crashedOnce = true;
          coordinator.shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
        } else {
          return;
        }
      } else if (key.endsWith("shutdown")) {
        coordinator.shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
      } else if (key.startsWith("-")) {
        stores.forEach(store -> store.delete(key.substring(1)));
      } else if (key.startsWith(":")) {
        // write the message and flush, but don't invoke commit later
        String msg = key.substring(1);
        stores.forEach(store -> store.put(msg, msg));
      } else {
        stores.forEach(store -> store.put(key, key));
      }
      stores.forEach(KeyValueStore::flush);

      if (!key.startsWith(":")) {
        coordinator.commit(TaskCoordinator.RequestScope.CURRENT_TASK);
      }
    }
  }
}
