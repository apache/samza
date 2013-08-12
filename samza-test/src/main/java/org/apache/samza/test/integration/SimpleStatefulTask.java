package org.apache.samza.test.integration;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

public class SimpleStatefulTask implements StreamTask, InitableTask {
  
  private KeyValueStore<String, String> store;
  
  public void init(Config config, TaskContext context) {
    this.store = (KeyValueStore<String, String>) context.getStore("mystore");
    System.out.println("Contents of store: ");
    KeyValueIterator<String, String> iter = store.all();
    while(iter.hasNext()) {
      Entry<String, String> entry = iter.next();
      System.out.println(entry.getKey() + " => " + entry.getValue());
    }
    iter.close();
  }
  
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    System.out.println("Adding " + envelope.getMessage() + " => " + envelope.getMessage() + " to the store.");
    store.put((String) envelope.getMessage(), (String) envelope.getMessage());
    coordinator.commit();
  }

}
