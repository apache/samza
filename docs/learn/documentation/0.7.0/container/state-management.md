---
layout: page
title: State Management
---

One of the more interesting aspects of Samza is the ability for tasks to store data locally and execute rich queries against it.

Of course simple filtering or single-row transformations can be done without any need for collecting state. A simple analogy to SQL may make this more obvious. The select- and where-clauses of a SQL query don't usually require state: these can be executed a row at a time on input data and maintain state between rows. The rest of SQL, multi-row aggregations and joins, require more support to execute correctly in a streaming fashion. Samza doesn't provide a high-level language like SQL but it does provide lower-level primitives that make streaming aggregation and joins and other stateful processing easy to implement.

Let's dive into how this works and why it is useful.

### Common use cases for stateful processing

First, let's look at some simplistic examples of stateful stream processing that might be seen on a consumer website. Later in this document we'll go through specific details of using Samza's built-in key-value storage capabilities to implement each of these applications, but for now it is enough just to see some examples of the kind of applications that tend to need to manage state.

##### Windowed aggregation

Example: Counting the number of page views for each user per hour

This kind of windowed processing is common for ranking and relevance, "trending topics", as well as simple real-time reporting and monitoring. For small windows one can just maintain the aggregate in memory and manually commit the task position only at window boundaries. However this means we have to recover up to a full window on fail-over. But this will not be very slow for large windows because of the amount of reprocessing. For larger windows or for effectively infinite windows it is better to make the in-process aggregation fault-tolerant rather than try to recompute it.

##### Table-table join

Example: Join a table of user profiles to a table of user\_settings by user\_id and emit the joined stream

This example is somewhat simplistic: one might wonder why you would want to join two tables in a stream processing system. However consider a more realistic example: real-time data normalization. E-commerce companies need to handle product imports, web-crawlers need to update their [database of the web](http://labs.yahoo.com/files/YahooWebmap.pdf), and social networks need to normalize and index social data for search. Each of these processing flows are immensely complex and contain many complex processing stages that effectively join together and normalize many data sources into a single clean feed.

##### Table-stream join

Example: Join user region information to page view data

Joining side-information to a real-time feed is a classic use for stream processing. It's particularly common in advertising, relevance ranking, fraud detection and other domains. Activity data such as page views are generally captured with only a few primary keys, the additional attributes about the viewer and viewed items that are needed for processing need to joined on after-the-fact.

##### Stream-stream join

Example: Join a stream of ad clicks to a stream of ad views to link the ad view that lead to the click

This is the classic stream join for "nearly aligned" streams. If the events that need to be joined arrive in a limited window it may be possible to buffer unjoined events in memory. Obviously this will be only approximate: any in-flight items will be lost if a machine crashes. However for more exact results, or to handle a very large window of misalignment, stateful processing is needed.

##### More

Of course there are infinite variations on joins and aggregations, but most amount to essentially variations and combinations of the above patterns.

### Approaches to managing task state

So how do systems support this kind of stateful processing? We'll lead in by describing what we have seen in other systems and then describe what Samza does.

#### In-memory state with checkpointing

A simple approach common in academic stream processing systems is to simply to periodically save out the state of the task's in-memory data. S4's [state management](http://incubator.apache.org/s4/doc/0.6.0/fault_tolerance) implements this approach&mdash;tasks implement Java's serializable interface and are periodically serialized using java serialization to save out copies of the processor state.

This approach works well enough if the in-memory state consists of only a few values. However since you have to save out the complete task state on each save this will become increasingly expensive as task state grows. Unfortunately most use cases we have seen revolve around joins and aggregation and so have large amounts of state&mdash;often many gigabytes. This makes periodic full dumps extremely impractical. Some academic systems handle this case by having the tasks produce "diffs" in addition to full checkpoints. However this requires a great deal of complexity in the task to track what has changed and efficiently produce a compact diff of changes.

#### Using an external store

In the absence of built-in support a common pattern for stateful processing is to push any state that would be accumulated between rows into an external database or key-value store. The database holds aggregates or the dataset being queried to enrich the incoming stream. You get something that looks like this:

![state-kv-store](/img/0.7.0/learn/documentation/container/stream_job_and_db.png)

Samza allows this style of processing (nothing will stop you from querying a remote database or service from your job) but also supports stateful processing natively in a way we think is often superior.

#### The problems of remote stores

To understand why this is useful let's first understand some of the drawbacks of making remote queries in a stream processing job:

1. **Performance**: The first major drawback of making remote queries is that they are slow and expensive. For example, a Kafka stream can deliver hundreds of thousands or even millions of messages per second per CPU core because it transfers large chunks of data at a time. But a remote database query is a more expensive proposition. Though the database may be partitioned and scalable this partitioning doesn't match the partitioning of the job into tasks so batching becomes much less effective. As a result you would expect to get a few thousand queries per second per core for remote requests. This means that adding a processing stage that uses an external database will often reduce the throughput by several orders of magnitude.
1. **Isolation**: If your database or service is also running live processing, mixing in asynchronous processing can be quite dangerous. A scalable stream processing system can run with very high parallelism. If such a job comes down (say for a code push) it queues up data for processing, when it restarts it will potentially have a large backlog of data to process. Since the job may actually have very high parallelism this can result in huge load spikes, many orders of magnitude higher than steady state load. If this load is mixed with live queries (i.e. the queries used to build web pages or render mobile ui or anything else that has a user waiting on the other end) then you may end up causing a denial-of-service attack on your live service.
1. **Query Capabilities**: Many scalable databases expose very limited query interfaces--only supporting simple key-value lookups. Doing the equivalent of a "full table scan" or rich traversal may not be practical in this model.
1. **Correctness**: If your task keeps counts or otherwise modifies state in a remote store how is this rolled back if the task fails? 

Where these issues become particularly problematic is when you need to reprocess data. Your output, after all, is a combination of your code and your input&mdash;when you change your code you often want to reprocess input to recreate the output state with the new improved code. This is generally quite reasonable for pure stream processing jobs, but generally impractical for performance and isolation reasons for jobs that make external queries.

### Local state in Samza

Samza allows tasks to maintain persistent, mutable, queryable state that is physically co-located with each task. The state is highly available: in the event of a task failure it will be restored when the task fails over to another machine.

You can think of this as taking the remote table out of the remote database and physically partitioning it up and co-locating these partitions with the tasks. This looks something like this:

![state-local](/img/0.7.0/learn/documentation/container/stateful_job.png)

Note that now the state is physically on the same machine as the tasks, and each task has access only to its local partition. However the combination of stateful tasks with the normal partitioning capabilities Samza offers makes this a very general feature: in general you just repartition on the key by which you want to split your processing and then you have full local access to the data within storage in that partition.

In cases where we were querying the external database on each input message to join on additional data for our output stream we would now instead create an input stream coming from the remote database that captures the changes to the database.

Let's look at how this addresses the problems of the remote store:

1. This fixes the performance issues of remote queries because the data is now local, what would otherwise be a remote query may now just be a lookup against local memory or disk (we ship a [LevelDB](https://code.google.com/p/leveldb)-based store which is described in detail below).
1. The isolation issues goes away as well as the queries are executed against the same servers the job runs against and this computation is not intermixed with live service calls.
1. Data is now local so any kind of data-intensive processing, scans, and filtering is now possible.
1. The store can abide by the same delivery and fault-tolerance guarantees that the Samza task itself does.

### Key-value storage

Though the storage format is pluggable, we provide a key-value store implementation to tasks out-of-the-box and gives the usual put/get/delete/range queries. This is backed by a highly available "changelog" stream that provides fault-tolerance by acting as a kind of [redo log](http://en.wikipedia.org/wiki/Redo_log) for the task's state (we describe this more in the next section).

This key-value storage engine is built on top of [LevelDB](https://code.google.com/p/leveldb). LevelDB has several nice properties. First it maintains data outside the java heap which means it is immediately preferable to any simple approach using a hash table both because of memory-efficiency and to avoid GC. It will use an off-heap memory cache and when that is exhausted go to disk for lookups&mdash;so small data sets can be [very fast](https://code.google.com/p/leveldb) and non-memory-resident datasets, though slower, are still possible. It is [log-structured](http://www.igvita.com/2012/02/06/sstable-and-log-structured-storage-leveldb/) and writes can be performed at close to disk speeds. It also does built-in block compression which helps to reduce both I/O and memory usage.

The nature of Samza's usage allows us to optimize this further. We add an optional "L1" LRU cache which is in-heap and holds deserialized rows. This cache is meant to be very small and let's us introduce several optimizations for both reads and writes.

The cache is an "object" or "row" cache&mdash;that is it maintains the java objects stored with no transformation or serialization. This complements LevelDB's own block level caching well. Reads and writes both populate the cache, and reads on keys in the cache avoid the cost of deserialization for these very common objects.

For writes the cache provides two benefits. Since LevelDB is itself really only a persistent "cache" in our architecture we do not immediately need to apply every write to the filesystem. We can batch together a few hundred writes and apply them all at once. LevelDB heavily optimizes this kind of batch write. This does not impact consistency&mdash;a task always reads what it wrote (since it checks the cache first). Secondly the cache effectively deduplicates updates so that if multiple updates to the same key occur close together we can optimize away all but the final write to leveldb and the changelog. For example, an important use case is maintaining a small number of counters that are incremented on every input. A naive implementation would need to write out each new value to LevelDB as well as perhaps logging the change out to the changelog for the task. In the extreme case where you had only a single variable, x, incremented on each input, an uncached implementation would produce writes in the form "x=1", "x=2", "x=3", etc which is quite inefficient. This is overkill, we only need to flush to the changelog at [commit points](checkpointing.html) not on every write. This allows us to "deduplicate" the writes that go to leveldb and the changelog to just the final value before the commit point ("x=3" or whatever it happened to be).

The combination of these features makes it possible to provide highly available processing that performs very close to memory speeds for small datasets yet still scales up to TBs of data (partitioned up across all the tasks).

### Fault-tolerance

As mentioned the actual local storage (i.e. LevelDB for key-value storage) is really just a cache. How can we ensure that this data is not lost when a machine fails and the tasks running on that machine have to be brought up on another machine (which, of course, doesn't yet have the local persistent state)?

The answer is that Samza handles state as just another stream. There are two mechanisms for accomplishing this.

The first approach is just to allow the task to replay one or more of its input streams to populate its store when it restarts. This works well if the input stream maintains the complete data (as a stream fed by a database table might) and if the input stream is fast enough to make this practical. This requires no framework support.

However often the state that is stored is much smaller than the input stream (because is it an aggregation or projection of the original input streams). Or the input stream may not maintain a complete, replayable set of inputs (say for event logs). To support these cases we provide the ability to back the state of the store with a changelog stream. A changelog is just a stream to which the task logs each change to its state&mdash;i.e. the sequence of key-value pairs applied to the local store. Changelogs are co-partitioned with their tasks (so each task has its own stream partition for which it is the only writer).

The changelogs are just normal streams&mdash;other downstream tasks can subscribe to this state and use it. And it turns out that very often the most natural way to represent the output of a job is as the changelog of its task (we'll show some examples in a bit).

Of course a log of changes only grows over time so this would soon become impractical. Kafka has [log-compaction](https://cwiki.apache.org/confluence/display/KAFKA/Log+Compaction) which provides special support for this kind of use case, though. This feature allows Kafka to compact duplicate entries (i.e. multiple updates with the same key) in the log rather than just deleting old log segments. This feature is new, it is in trunk and will be released soon as part of the 0.8.1 release.

The Kafka brokers scale well up to terabytes of data per machine for changelogs as for other topics. Log compaction proceeds at about 50MB/sec/core or whatever the I/O limits of the broker are.

### Other storage engines

One interesting aspect of this design is that the fault-tolerance mechanism is completely decoupled from the query apis the storage engine provides to the task or the way it stores data on disk or in memory. We have provided a key-value index with Samza, but you can easily implement and plug-in storage engines that are optimized for other types of queries and plug them in to our fault tolerance mechanism to provide different query capabilities to your tasks.

Here are a few examples of storage engine types we think would be interesting to pursue in the future (patches accepted!):

##### Persistent heap

A common operation in stream processing is to maintain a running top-N. There are two primary applications of this. The first is ranking items over some window. The second is performing a "bounded sort" operation to transform a nearly sorted input stream into a totally sorted output stream. This occurs when dealing with a data stream where the order is by arrival and doesn't exactly match the source timestamp (for example log events collected across many machines).

##### Sketches

Many applications don't require exact results and for these [approximate algorithms](http://infolab.stanford.edu/~ullman/mmds/ch4.pdf) such as [bloom filters](http://en.wikipedia.org/wiki/Bloom_filter) for set membership, [hyperloglog](http://research.google.com/pubs/pub40671.html) for counting distinct keys, and a multitude of algorithms for quantile and histogram approximation.

These algorithms are inherently approximate but good algorithms give a strong bound on the accuracy of the approximation. This obviously doesn't carry over well to the case where the task can crash and lose all state. By logging out the changes to the structure we can ensure it is restored on fail-over. The nature of sketch algorithms allows significant opportunity for optimization in the form of logging. 

##### Inverted index

Inverted indexes such as is provided by [Lucene](http://lucene.apache.org) are common for text matching and other applications that do matching and ranking with selective queries and large result sets. 

##### More

There are a variety of other storage engines that could be useful:

* For small datasets logged, in-memory collections may be ideal.
* Specialized data structures for graph traversal are common.
* Many applications are doing OLAP-like aggregations on their input. It might be possible to optimize these kinds of dimensional summary queries.

### Using the key-value store

In this section we will give a quick tutorial on configuring and using the key-value store.

To declare a new store for usage you add the following to your job config:

    # Use the key-value store implementation for a store called "my-store"
    stores.my-store.factory=samza.storage.kv.KeyValueStorageEngineFactory

    # Log changes to the store to an output stream for restore
    # If no changelog is specified the store will not be logged (but you can still rebuild off your input streams)
    stores.my-store.changelog=my-stream-name

    # The system to use for the changelog stream
    stores.my-store.system=kafka

    # The serialization format to use
    stores.my-store.serde=string

Here is some simple example code that only writes to the store:

    public class MyStatefulTask implements StreamTask, InitableTask {
      private KeyValueStore<String, String> store;
      
      public void init(Config config, TaskContext context) {
        this.store = (KeyValueStore<String, String>) context.getStore("store");
      }

      public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        System.out.println("Adding " + envelope.getKey() + " => " + envelope.getMessage() + " to the store.");
        store.put((String) envelope.getKey(), (String) envelope.getMessage());
      }
    }

Here is the complete key-value store API:

    public interface KeyValueStore<K, V> {
      V get(K key);
      void put(K key, V value);
      void putAll(List<Entry<K,V>> entries);
      void delete(K key);
      KeyValueIterator<K,V> range(K from, K to);
      KeyValueIterator<K,V> all();
    }

Here is a list of additional configurations accepted by the key-value store along with their default values:

    # The number of writes to batch together
    stores.my-store.batch.size=500

    # The total number of objects to cache in the "L1" object cache. This must be at least as large as the batch.size.
    # A cache size of 0 disables all caching and batching.
    stores.my-store.object.cache.size=1000

    # The size of the off-heap leveldb block cache in bytes
    stores.my-store.leveldb.block.cache.size=16777216

    # Enable block compression using snappy?
    stores.my-store.leveldb.compress=true

    # The remaining options are a bit low-level and likely you don't need to change them unless you are a compulsive fiddler

    # The leveldb block size (see leveldb docs).
    stores.my-store.leveldb.block.size=4096

    # The amount of memory leveldb uses for buffering the latest segment, also the size of leveldb's segment files. 
    stores.my-store.leveldb.write.buffer.size=16777216

### Implementing common use cases with the key-value store

Let's look at how you can address some of the common use-cases we discussed before using the key-value storage engine.

##### Windowed aggregation

Example: Counting the number of page views for each user per hour

Implementation: We have two processing stages. The first partitions the input data by user id (if it's already partitioned by user id, which would be reasonable, you can skip this), and the second stage does the counting. The job has a single store containing the mapping of user_id to the running count. Each new input record would cause the job to retrieve the current running count, increment it and write back the count. When the window is complete (i.e. the hour is over), we iterate over the contents of our store and emit the aggregates.

One thing to note is that this job effectively pauses at the hour mark to output its results. This is unusual for stream processing, but totally fine for Samza&mdash;and we have specifically designed for this case. Scans over the contents of the key-value store will be quite fast and input data will buffer while the job is doing this scanning and emitting aggregates.

##### Table-table join

Example: Join a table of user profiles to a table of user settings by user\_id and emit the joined stream

Implementation: The job subscribes to the change stream for user profiles and for user settings databases, both partitioned by user\_id. The job keeps a single key-value store keyed by user\_id containing the joined contents of profiles and settings. When a new record comes in from either stream it looks up the current value in its store and writes back the record with the appropriate fields updated (i.e. new profile fields if it was a profile update, and new settings fields if it was a settings update). The changelog of the store doubles as the output stream of the task.

##### Table-stream join

Example: Join user zip code to page view data (perhaps to allow aggregation by zip code in a later stage)

Implementation: The job subscribes to the user profile stream and page view stream. Each time it gets a profile update it stores the zipcode keyed by user\_id. Each time a page view arrives it looks up the zip code for the user and emits the enriched page view + zipcode event.

##### Stream-stream join

Example: Join ad clicks to ad impressions by impression id (an impression is advertising terminology for the event that records the display of an ad)

Note: In this example we are assuming that impressions are assigned a unique guid and this is present in both the original impression event and any subsequent click. In the absence of this the business logic for choosing the join could be substituted for the simple lookup.

Implementation: Partition the ad click and ad impression streams by the impression id or user id. The task keeps a store of unmatched clicks and unmatched impressions. When a click comes in we try to find its matching impression in the impression store, and vice versa. If a match is found emit the joined pair and delete the entry. If no match is found store the event to wait for a match. Since this is presumably a left outer join (i.e. every click has a corresponding impression but not vice versa) we will periodically scan the impression table and delete old impressions for which no click arrived.

### Databases as streams

One assumption we are making in the above is that you can extract a stream of changes from your databases. This could be done by publishing a stream of changes to Kafka or by implementing our [pluggable stream interface](streams.html) to directly poll the database for changes. You want this to be done in a way that you can reset the offset or timestamp back to zero to replay the current state of the database as changes if you need to reprocess. If this stream capture is efficient enough you can often avoid having changelogs for your tasks and simply replay them from the source when they fail.

A wonderful contribution would be a generic jdbc-based stream implementation for extracting changes from relational databases.

### Implementing storage engines

We mentioned that the storage engine interface was pluggable. Of course you can use any data structure you like in your task provided you can repopulate it off your inputs on failure. However to plug into our changelog infrastructure you need to implement a generic StorageEngine interface that handles restoring your state on failure and ensures that data is flushed prior to commiting the task position.

The above code shows usage of the key-value storage engine, but it is not too hard to implement an alternate storage engine. To do so, you implement methods to restore the contents of the store from a stream, flush any cached content on commit, and close the store:

    public interface StorageEngine {
      void restore(Iterator<IncomingMessageEnvelope> envelopes);
      void flush();
      void stop();
    }

The user specifies the type of storage engine they want by passing in a factory for that store in their configuration.

### Fault tolerance semantics with state

Samza currently only supports at-least-once delivery guarantees in the presence of failure (this is sometimes referred to as "guaranteed delivery"). This means messages are not lost but if a task fails some messages may be redelivered. The guarantee holds from the commit point of the task which records the position from which the task will restart on failure (the user can either force a commit at convenient points or the framework will by default do this at regular intervals). This is true for both input, output, and changelog streams. This is a fairly weak guarantee&mdash;duplicates can give incorrect results in counts, for example. We have a plan to extend this to exact semantics in the presence of failure which we will include in a future release.

## [Metrics &raquo;](metrics.html)
