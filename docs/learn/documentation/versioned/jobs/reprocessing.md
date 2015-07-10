---
layout: page
title: Reprocessing previously processed data
---
<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

From time to time you may want to deploy a new version of your Samza job that computes results differently. Perhaps you fixed a bug or introduced a new feature. For example, say you have a Samza job that classifies messages as spam or not-spam, using a machine learning model that you train offline. Periodically you want to deploy an updated version of your Samza job which includes the latest classification model.

When you start up a new version of your job, a question arises: what do you want to do with messages that were previously processed with the old version of your job? The answer depends on the behavior you want:

1. **No reprocessing:** By default, Samza assumes that messages processed by the old version don't need to be processed again. When the new version starts up, it will resume processing at the point where the old version left off (assuming you have [checkpointing](../container/checkpointing.html) enabled). If this is the behavior you want, there's nothing special you need to do.

2. **Simple rewind:** Perhaps you want to go back and re-process old messages using the new version of your job. For example, maybe the old version of your classifier marked things as spam too aggressively, so you now want to revisit its previous spam/not-spam decisions using an improved classifier. You can do this by restarting the job at an older point in time in the stream, and running through all the messages since that time. Thus your job starts off reprocessing messages that it has already seen, but it then seamlessly continues with new messages when the reprocessing is done.

   This approach requires an input system such as Kafka, which allows you to jump back in time to a previous point in the stream. We discuss below how this works in practice.

3. **Parallel rewind:** This approach avoids a downside of the *simple rewind* approach. With simple rewind, any new messages that appear while the job is reprocessing old data are queued up, and are processed when the reprocessing is done. The queueing delay needn't be long, because Samza can stream through historical data very quickly, but some latency-sensitive applications need to process messages faster.

   In the *parallel rewind* approach, you run two jobs in parallel: one job continues to handle live updates with low latency (the *real-time job*), while the other is started at an older point in the stream and reprocesses historical data (the *reprocessing job*). The two jobs consume the same input stream at different points in time, and eventually the reprocessing job catches up with the real-time job.

   There are a few details that you need to think through before deploying parallel rewind, which we discuss below.

### Jumping Back in Time

A common aspect of the *simple rewind* and *parallel rewind* approaches is: you have a job which jumps back to an old point in time in the input streams, and consumes all messages since that time. You achieve this by working with Samza's checkpoints.

Normally, when a Samza job starts up, it reads the latest checkpoint to determine at which offset in the input streams it needs to resume processing. If you need to rewind to an earlier time, you do that in one of two ways:

1. You can stop the job, manipulate its last checkpoint to point to an older offset, and start the job up again. Samza includes a command-line tool called [CheckpointTool](../container/checkpointing.html#toc_0) which you can use to manipulate checkpoints.
2. You can start a new job with a different *job.name* or *job.id* (e.g. increment *job.id* every time you need to jump back in time). This gives the job a new checkpoint stream, with none of the old checkpoint information. You also need to set [samza.offset.default=oldest](../container/checkpointing.html), so that when the job starts up without checkpoint, it starts consuming at the oldest offset available.

With either of these approaches you can get Samza to reprocess the entire history of messages in the input system. Input systems such as Kafka can retain a large amount of history &mdash; see discussion below. In order to speed up the reprocessing of historical data, you can increase the container count (*yarn.container.count* if you're running Samza on YARN) to boost your job's computational resources.

If your job maintains any [persistent state](../container/state-management.html), you need to be careful when jumping back in time: resetting a checkpoint does not automatically change persistent state, so you could end up reprocessing old messages while using state from a later point in time. In most cases, a job that jumps back in time should start with an empty state. You can reset the state by deleting the changelog topic, or by changing the name of the changelog topic in your job configuration.

When you're jumping back in time, you're using Samza somewhat like a batch processing framework (e.g. MapReduce) &mdash; with the difference that your job doesn't stop when it has processed all the historical data, but instead continues running, incrementally processing the stream of new messages as they come in. This has the advantage that you don't need to write and maintain separate batch and streaming versions of your job: you can just use the same Samza API for processing both real-time and historical data.

### Retention of history

Samza doesn't maintain history itself &mdash; that is the responsibility of the input system, such as Kafka. How far back in time you can jump depends on the amount of history that is retained in that system.

Kafka is designed to keep a fairly large amount of history: it is common for Kafka brokers to keep one or two weeks of message history accessible, even for high volume topics. The retention period is mostly determined by how much disk space you have available. Kafka's performance [remains high](http://engineering.linkedin.com/kafka/benchmarking-apache-kafka-2-million-writes-second-three-cheap-machines) even if you have terabytes of history.

There are two different kinds of history which require different configuration:

* **Activity events** are things like user tracking events, web server log events and the like. This kind of stream is typically configured with a time-based retention, e.g. a few weeks. Events older than the retention period are deleted (or archived in an offline system such as HDFS).
* **Database changes** are events that show inserts, updates and deletes in a database. In this kind of stream, each event typically has a primary key, and a newer event for a key overwrites any older events for the same key. If the same key is updated many times, you're only really interested in the most recent value. (The [changelog streams](../container/state-management.html) used by Samza's persistent state fall in this category.)

In a database change stream, when you're reprocessing data, you typically want to reprocess the entire database. You don't want to miss a value just because it was last updated more than a few weeks ago. In other words, you don't want change events to be deleted just because they are older than some threshold. In this case, when you're jumping back in time, you need to rewind to the *beginning of time*, to the first change ever made to the database (known in Kafka as "offset 0").

Fortunately this can be done efficiently, using a Kafka feature called [log compaction](http://kafka.apache.org/documentation.html#compaction). 

For example, imagine your database contains counters: every time something happens, you increment the appropriate counters and update the database with the new counter values. Every update is sent to the changelog, and because there are many updates, the changelog stream will take up a lot of space. With log compaction turned on, Kafka deduplicates the stream in the background, keeping only the most recent counter value for each key, and deleting any old values for the same counter. This reduces the size of the stream so much that you can keep the most recent update for every key, even if it was last updated long ago.

With log compaction enabled, the stream of database changes becomes a full copy of the entire database. By jumping back to offset 0, your Samza job can scan over the entire database and reprocess it. This is a very powerful way of building scalable applications.

### Details of Parallel Rewind

If you are taking the *parallel rewind* approach described above, running two jobs in parallel, you need to configure them carefully to avoid problems. In particular, some things to look out for:

* Make sure that the two jobs don't interfere with each other. They need different *job.name* or *job.id* configuration properties, so that each job gets its own checkpoint stream. If the jobs maintain [persistent state](../container/state-management.html), each job needs its own changelog (two different jobs writing to the same changelog produces undefined results).
* What happens to job output? If the job sends its results to an output stream, or writes to a database, then the easiest solution is for each job to have a separate output stream or database table. If they write to the same output, you need to take care to ensure that newer data isn't overwritten with older data (due to race conditions between the two jobs).
* Do you need to support A/B testing between the old and the new version of your job, e.g. to test whether the new version improves your metrics? Parallel rewind is ideal for this: each job writes to a separate output, and clients or consumers of the output can read from either the old or the new version's output, depending on whether a user is in test group A or B.
* Reclaiming resources: you might want to keep the old version of your job running for a while, even when the new version has finished reprocessing historical data (especially if the old version's output is being used in an A/B test). However, eventually you'll want to shut it down, and delete the checkpoint and changelog streams belonging to the old version.

Samza gives you a lot of flexibility for reprocessing historical data, and you don't need to program against a separate batch processing API to take advantage of it. If you're mindful of these issues, you can build a data system that is very robust, but still gives you lots of freedom to change your processing logic in future.

## [Web UI and REST API &raquo;](web-ui-rest-api.html)
