---
layout: page
title: Configuration
---

All Samza jobs have a configuration file that defines the job. A very basic configuration file looks like this:

```
# Job
job.factory.class=samza.job.local.LocalJobFactory
job.name=hello-world

# Task
task.class=samza.task.example.MyJavaStreamerTask
task.inputs=example-stream

# Serializers
serializers.registry.json.class=samza.serializers.JsonSerdeFactory
serializers.default=json

# Streams
streams.example-stream.system=example-system
streams.example-stream.stream=some-stream

# Systems
systems.example-system.samza.consumer.factory=samza.stream.example.ExampleConsumerFactory
systems.example-system.samza.partition.manager=samza.stream.example.ExamplePartitionManager
```

There are five major sections to a configuration file. The job section defines things like the name of the job, and whether to use the YarnJobFactory or LocalJobFactory. The task section is where you specify the class name for your StreamTask. It's also where you define what the input streams are for your task. The system section defines systems that you can read from. Usually, you'll define a Kafka system, if you're reading from Kafka. After that you'll need to define the stream(s) that you want to read from, which systems they're coming from, and how to deserialize objects from the stream.

### Required Configuration

Configuration keys that absolutely must be defined for a Samza job are:

* job.factory.class
* job.name
* task.class
* task.inputs

### Configuration Keys

A complete list of configuration keys can be found on the [Configuration Table](configuration-table.html) page.

## [Packaging &raquo;](packaging.html)
