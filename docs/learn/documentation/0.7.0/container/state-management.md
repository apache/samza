---
layout: page
title: State Management
---

Samza allows tasks to maintain persistent, mutable state that is physically co-located with each task. The state is highly available: in the event of a task failure it will not be lost when the task fails over to another machine.

A key-value store implementation is provided out of the box that covers many use cases. Other store implementations can be plugged in for different types of storage.

State is naturally partitioned with the tasks, with one store per task. When there is a backing changelog, the stream will also be co-partitioned with the tasks. Possible extensions to handle non-partitioned state (i.e. a global lookup dictionary) are discussed at the end.

Restoring state can be done either by having a dedicated stream that captures the changes to the local store, or by rebuilding the state off the input streams.

### Use Cases

We have a few use-cases in mind for this functionality.

#### Windowed Aggregation

Example: Counting member page views by hour

Implementation: The stream is partitioned by the aggregation key (member\_id). Each new input record would cause the job to retrieve and update the aggregate (the page view count). When the window is complete (i.e. the hour is over), the job outputs the current stored aggregate value.

####Table-Table Join

Example: Join profile to user\_settings by member\_id and emit the joined stream

Implementation: The job subscribes to the change stream for profile and for user\_settings both partitioned by member\_id. The job keeps a local store containing both the profile and settings data. When a record comes in from either profile or settings, the job looks up the value for that member and updates the appropriate section (either profile or settings). The changelog for the local store can be used as the output stream if the desired output stream is simply the join of the two inputs.

#### Table-Stream Join

Example: Join member geo region to page view data

Implementation: The job subscribes to the profile stream (for geo) and page views stream, both partitioned by member\_id. It keeps a local store of member\_id => geo that it updates off the profile feed. When a page view arrives it does a lookup in this store to join on the geo data.

#### Stream-Stream Join

Example: Join ad clicks to ad impressions by some shared key

Implementation: Partition ad click and ad impression by the join key. Keep a store of unmatched clicks and unmatched impressions. When a click comes in try to find its matching impression in the impression store, and vice versa when an impression comes in check the click store. If a match is found emit the joined pair and delete the entry. If no match is found store the event to wait for a match. Since this is presumably a left outer join (i.e. every click has a corresponding impression but not vice versa) we will periodically scan the impression table and delete old impressions for which no click arrived.

#### More

Of course there are infinite variations on joins and aggregations, but most amount to essentially variations on the above.

### Usage

To declare a new store for usage you add the following to your job config:

    # Use the key-value store implementation for 
    stores.my-store.factory=samza.storage.kv.KeyValueStorageEngineFactory
    # Log changes to the store to a stream
    stores.my-store.changelog=my-stream-name
    # The serialization format to use
    stores.my-store.serde=string
    # The system to use for the changelog
    stores.my-store.system=kafka

Example code:

    public class MyStatefulTask implements StreamTask, InitableTask {
      private KeyValueStore<String, String> store;
      
      public void init(Config config, TaskContextPartition context) {
        this.store = (KeyValueStore<String, String>) context.getStore("store");
      }

      public void process(MessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        System.out.println("Adding " + envelope.getMessage() + " => " + envelope.getMessage() + " to the store.");
        store.put((String) envelope.getMessage(), (String) envelope.getMessage());
      }
    }

This shows the put() API, but KeyValueStore gives a fairly general key-value interface:

    public interface KeyValueStore<K, V> {
      V get(K key);
      void put(K key, V value);
      void putAll(List<Entry<K,V>> entries);
      void delete(K key);
      KeyValueIterator<K,V> range(K from, K to);
      KeyValueIterator<K,V> all();
    }

### Implementing Storage Engines

The above code shows usage of the key-value storage engine, but it is not too hard to implement an alternate storage engine. To do so, you implement methods to restore the contents of the store from a stream, flush any cached content on commit, and close the store:

    public interface StorageEngine {
      void restore(StreamConsumer consumer);
      void flush();
      void close();
    }

The user specifies the type of storage engine they want by passing in a factory for that store in their configuration.

### Fault Tolerance Semantics with State

Samza currently only supports at-least-once delivery guarantees. We will extend this to exact atomic semantics across outputs to multiple streams/partitions in the future.

<!-- TODO add fault tolerance semantics SEP link when one exists
The most feasible plan for exact semantics seems to me to be journalling non-deterministic decisions proposal outlined in the fault-tolerance semantics wiki. I propose we use that as a working plan.

To ensure correct semantics in the presence of faults we need to ensure that the task restores to the exact state at the time of the last commit.

If the task is fed off replayable inputs then it can simply replay these inputs to recreate its state.

If the task has a changelog to log its state then there is the possibility that the log contains several entries beyond the last commit point. The store should only restore up to the last commit point to ensure that the state is in the correct position with respect to the inputs–the remaining changelog will then be repeated and de-duplicated as the task begins executing.
-->

### Shared State

Originally we had discussed possibly allowing some facility for global lookup dictionaries that are un-partitioned; however, this does not work with our fault-tolerance semantics proposal, as the container-wide state changes out of band (effectively acting like a separate database or service). This would not work with proposed message de-duplication features since the task output is not deterministic.

## [Metrics &raquo;](metrics.html)
