---
layout: page
title: How to use Samza SQL
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

There are couple of ways to use Samza SQL

1. Run Samza SQL on your local machine.
2. Run Samza SQL on YARN.

# Running Samza SQL on your local machine  


Samza SQL console tool documented [here](samza-tools.html) uses Samza standalone to run the Samza SQL on your local machine. This is the quickest way to play with Samza SQL. Please follow the instructions [here](samza-tools.html) to get access to the Samza tools on your machine.

## Start the Kafka server

Please follow the instructions from the [Kafka quickstart](http://kafka.apache.org/quickstart) to start the zookeeper and Kafka server.

## Create ProfileChangeStream Kafka topic

The below sql statements requires a topic named ProfileChangeStream to be created on the Kafka broker. You can follow the instructions in the [Kafka quick start guide](http://kafka.apache.org/quickstart) to create a topic named "ProfileChangeStream".

```bash
./deploy/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic ProfileChangeStream
```

## Generate events into ProfileChangeStream topic

Use generate-kafka-events from [Samza tools](samza-tools.html) to generate events into the ProfileChangeStream

```bash
cd samza-tools-<version>
./scripts/generate-kafka-events.sh -t ProfileChangeStream -e ProfileChange
```

## Using Samza SQL Console to run Samza sql on your local machine

Below are some of the sql queries that you can execute using the samza-sql-console tool from [Samza tools](samza-tools.html) package.

```bash
# This command just prints out all the events in the Kafka topic ProfileChangeStream into console output as a json serialized payload.
./scripts/samza-sql-console.sh --sql "insert into log.consoleoutput select * from kafka.ProfileChangeStream"

# This command prints out the fields that are selected into the console output as a json serialized payload.
./scripts/samza-sql-console.sh --sql "insert into log.consoleoutput select Name, OldCompany, NewCompany from kafka.ProfileChangeStream"

# This command showcases the RegexMatch udf and filtering capabilities.
./scripts/samza-sql-console.sh --sql "insert into log.consoleoutput select Name as __key__, Name, NewCompany, RegexMatch('.*soft', OldCompany) from kafka.ProfileChangeStream where NewCompany = 'LinkedIn'"
```

# Running Samza SQL on YARN

The [hello-samza](https://github.com/apache/samza-hello-samza) project is an example project designed to help you run your first Samza application. It has examples of applications using the Low Level  Task API, High Level Streams API as well as Samza SQL.

This tutorial demonstrates a simple Samza application that uses SQL to perform stream processing.

## Get the hello-samza Code and Start the grid

Please follow the instructions from [hello-samza-high-level-yarn](hello-samza-high-level-yarn.html) on how to build the hello-samza repository and start the yarn grid. 

## Create the topic and generate Kafka events

Please follow the steps in the section "Create ProfileChangeStream Kafka topic" and "Generate events into ProfileChangeStream topic" above.

## Build a Samza Application Package

Before you can run a Samza application, you need to build a package for it. Please follow the instructions from [hello-samza-high-level-yarn](hello-samza-high-level-yarn.html) on how to build the hello-samza application package.

## Run a Samza Application

After you've built your Samza package, you can start the app on the grid using the run-app.sh script.

```bash
./deploy/samza/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/page-view-filter-sql.properties
```

The app executes the following SQL command :
```sql
insert into kafka.NewLinkedInEmployees select Name from ProfileChangeStream where NewCompany = 'LinkedIn'
```

This SQL performs the following

1. Consumes the Kafka topic ProfileChangeStreamStream which contains the avro serialized ProfileChangeEvent(s) 
2. Deserializes the events and filters out only the profile change events where NewCompany = 'LinkedIn' i.e. Members who have moved to LinkedIn.
3. Writes the Avro serialized event that contains the Id and Name of those profiles to Kafka topic NewLinkedInEmployees.


Give the job a minute to startup, and then tail the Kafka topic:

```bash
./deploy/kafka/bin/kafka-console-consumer.sh  --zookeeper localhost:2181 --topic NewLinkedInEmployees
```

Congratulations! You've now setup a local grid that includes YARN, Kafka, and ZooKeeper, and run a Samza SQL application on it.

## Shutdown and cleanup

To shutdown the app, use the same _run-app.sh_ script with an extra _--operation=kill_ argument
```bash
./deploy/samza/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/page-view-filter-sql.properties --operation=kill
```

Please follow the instructions from [Hello Samza High Level API - YARN Deployment](hello-samza-high-level-yarn.html) on how to shutdown and cleanup the app.
