---
layout: page
title: Batch Processing Overview
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

Samza provides a unified data processing model for both stream and batch processing. In Samza batch is treated as a special case of streaming: the sources are bounded data sets in batch, whereas unbounded data sets in streaming. Under the hood, it’s the same highly-efficient stream-processing engine.

![diagram-medium](/img/{{site.version}}/learn/documentation/hadoop/unified_batch_streaming.png)

### Unified API for Batch and Streaming

Samza provides a single set of APIs for both batch and stream processing. This unified programming API makes it convenient for you to focus on the processing logic, without treating bounded source and unbounded source differently. Switching between batch and streaming only requires config change, e.g. [Kafka](../api/overview.html) to [HDFS](./consumer.html), instead of any code change.

### Multi-stage Batch Pipeline

Complex data pipelines usually consist multiple stages, with data shuffled (repartitioned) across stages for various windowing, aggregation and join. Samza [high-level API](/startup/preview/index.html) provides a useful operator named `partitionBy` to help the repartitioning of the data. Internally, Samza creates a physical stream, called an “intermediate stream”, sends the previous stage output to the stream, and then feed it to the next stage of the pipeline. The lifecycle of intermediate streams is completely managed by Samza so from the user perspective the data shuffling is automatic.

For a single-stage pipeline, dealing with bounded data sets is straightforward: the system consumer “knows” the end of a particular partition, and it will emit end-of-stream token once a partition is complete. Samza will shut down the container when all its input partitions are complete.

For a multi-stage pipeline, however, things become tricky since intermediate streams are often physically unbounded data streams, e.g. Kafka, and the downstream stages don't know when to shut down since unbounded streams don't have an end. To solve this problem, Samza uses in-band end-of-stream control messages in the intermediate stream along with user data messages. The upstream stage broadcasts end-of-stream control messages to every partition of the intermediate stream, and the downstream stage will aggregate the messages received and conclude an intermediate stream partition reaches to the end once the aggregation result matches all its upstream tasks. The following diagram shows the flow:

![diagram-medium](/img/{{site.version}}/learn/documentation/hadoop/multi_stage_batch.png)

### State and Fault-tolerance

Samza’s [state management](../container/state-management.html) and [fault-tolerance](../container/checkpointing.html) apply the same to batch. You can use in-memory or RocksDb as your local state store which can be persisted by changelog streams. In case of any container failures, Samza will restart the container by reseeding the local store from changelog streams, and resume processing from the previous checkpoints.

During a job restart, batch processing behaves completely different from streaming. In batch, it is expected to be a fresh re-run and all the internal streams, including intermediate, checkpoint and changelog streams, need to be cleaned up before the run. To achieve this, Samza assigns a unique **run.id** to each job run. The **run.id** is appended to the physical stream name when Samza creates these internal topics. So for each run, the job will have a new set of internal streams to work with. Samza also performs due diligence to delete/purge the streams from previous run.

## [Reading from HDFS &raquo;](./consumer.html)