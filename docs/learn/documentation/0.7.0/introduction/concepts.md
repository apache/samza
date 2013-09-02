---
layout: page
title: Concepts
---

This page gives an introduction to the high-level concepts in Samza.

### Streams

Samza processes *streams*. A stream is composed of immutable *messages* of a similar type or category. Example streams might include all the clicks on a website, or all the updates to a particular database table, or any other type of event data. Messages can be appended to a stream or read from a stream. A stream can have any number of readers and reading from a stream doesn't delete the message so a message written to a stream is effectively broadcast out to all readers. Messages can optionally have an associated key which is used for partitioning, which we'll talk about in a second.

Samza supports pluggable *systems* that implement the stream abstraction: in Kafka a stream is a topic, in a database we might read a stream by consuming updates from a table, in Hadoop we might tail a directory of files in HDFS.

![job](/img/0.7.0/learn/documentation/introduction/job.png)

### Jobs

A Samza *job* is code that performs a logical transformation on a set of input streams to append output messages to set of output streams.

If scalability were not a concern streams and jobs would be all we would need. But to let us scale our jobs and streams we chop these two things up into smaller unit of parallelism below the stream and job, namely *partitions* and *tasks*.

### Partitions

Each stream is broken into one or more partitions. Each partition in the stream is a totally ordered sequence of messages.

Each position in this sequence has a unique identifier called the *offset*. The offset can be a sequential integer, byte offset, or string depending on the underlying system implementation.

Each message appended to a stream is appended to only one of the streams partitions. The assignment of the message to its partition is done with a key chosen by the writer (in the click example above, data might be partitioned by user id).

![stream](/img/0.7.0/learn/documentation/introduction/stream.png)

### Tasks

A job is itself distributed by breaking it into multiple *tasks*. The *task* is the unit of parallelism of the job, just as the partition is to the stream. Each task consumes data from one partition for each of the job's input streams.

The task processes messages from each of its input partitions *in order by offset*. There is no defined ordering between partitions.

The position of the task in its input partitions can be represented by a set of offsets, one for each partition.

The number of tasks a job has is fixed and does not change (though the computational resources assigned to the job may go up and down). The number of tasks a job has also determines the maximum parallelism of the job as each task processes messages sequentially. There cannot be more tasks than input partitions (or there would be some tasks with no input).

The partitions assigned to a task will never change: if a task is on a machine that fails the task will be restarted elsewhere still consuming the same stream partitions.

![job-detail](/img/0.7.0/learn/documentation/introduction/job_detail.png)

### Dataflow Graphs

We can compose multiple jobs to create data flow graph where the nodes are streams containing data and the edges are jobs performing transformations. This composition is done purely through the streams the jobs take as input and output&mdash;the jobs are otherwise totally decoupled: They need not be implemented in the same code base, and adding, removing, or restarting a downstream job will not impact an upstream job.

These graphs are often acyclic&mdash;that is, data usually doesn't flow from a job, through other jobs, back to itself. However this is not a requirement.

![dag](/img/0.7.0/learn/documentation/introduction/dag.png)

### Containers

Partitions and tasks are both *logical* units of parallelism, they don't actually correspond to any particular assignment of computational resources (CPU, memory, disk space, etc). Containers are the unit of physical parallelism, and a container is essentially just a unix process (or linux [cgroup](http://en.wikipedia.org/wiki/Cgroups)). Each container runs one or more tasks. The number of tasks is determined automatically from the number of partitions in the input and is fixed, but the number of containers (and the cpu and memory resources associated with them) is specified by the user at run time and can be changed at any time.

## [Architecture &raquo;](architecture.html)
