package org.apache.samza.operators.impl;

import org.apache.commons.lang.NotImplementedException;
import org.apache.samza.operators.data.Message;
import org.apache.samza.operators.internal.Operators;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * The base class for all state stores
 */
public class SessionStateStoreImpl<M extends Message, SK, SS> {
  private final String storeName;
  private final Operators.StoreFunctions<M, SK, SS> storeFunctions;
  private KeyValueStore<SK, SS> kvStore = null;
  private boolean earlyTriggerFired = false;

  public SessionStateStoreImpl(Operators.StoreFunctions<M, SK, SS> store, String storeName) {
    this.storeFunctions = store;
    this.storeName = storeName;
  }

  public void init(TaskContext context) {
    this.kvStore = new OrderPreservingKeyValueStore<>();
  }

  public Entry<SK, SS> getState(M m) {
    SK key = this.storeFunctions.getStoreKeyFinder().apply(m);
    SS state = this.kvStore.get(key);
    return new Entry<>(key, state);
  }

  public Entry<SK, SS> updateState(M m) {
    Entry<SK, SS> oldEntry = getState(m);
    SS newValue = this.storeFunctions.getStateUpdater().apply(m, oldEntry.getValue());
    this.kvStore.put(oldEntry.getKey(), newValue);
    return new Entry<>(oldEntry.getKey(), newValue);
  }

  public KeyValueIterator<SK, SS> getIterator() {
    return kvStore.all();
  }

  public void delete(SK key) {
    kvStore.delete(key);
  }


}
