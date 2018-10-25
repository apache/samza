---
layout: page
title: Samza SQL
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


# Overview
Samza SQL allows users to write Stream processing application by just writing a SQL query. SQL query is translated to a Samza job using the high level API, which can then be run on all the environments (e.g. Yarn, standalone, etc..) that Samza currently supports.

Samza SQL was created with following principles:

1. Users of Samza SQL should be able to create stream processing apps without needing to write Java code unless they require UDFs.
2. No configs needed by users to develop and execute a Samza SQL job.
3. Samza SQL should support all the runtimes and systems that Samza supports.   

Samza SQL uses Apache Calcite to provide the SQL interface. Apache Calcite is a popular SQL engine used by wide variety of big data processing systems (Beam, Flink, Storm etc..)

# How to use Samza SQL
There are couple of ways to use Samza SQL:

* Run Samza SQL on your local machine.
* Run Samza SQL on YARN.

# Running Samza SQL on your local machine
Samza SQL console tool documented [here](https://samza.apache.org/learn/tutorials/0.14/samza-tools.html) uses Samza standalone to run Samza SQL on your local machine. This is the quickest way to play with Samza SQL. Please follow the instructions [here](https://samza.apache.org/learn/tutorials/0.14/samza-tools.html) to get access to the Samza tools on your machine.

## Start the Kafka server
Please follow the instructions from the [Kafka quickstart](http://kafka.apache.org/quickstart) to start the zookeeper and Kafka server.

## Create ProfileChangeStream Kafka topic
The below sql statements require a topic named ProfileChangeStream to be created on the Kafka broker. You can follow the instructions in the Kafka quick start guide to create a topic named “ProfileChangeStream”.

```bash
>./deploy/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic ProfileChangeStream
```

## Generate events into ProfileChangeStream topic
Use generate-kafka-events from Samza tools to generate events into the ProfileChangeStream

```bash
> cd samza-tools-<version>
> ./scripts/generate-kafka-events.sh -t ProfileChangeStream -e ProfileChange
```

## Using Samza SQL Console to run Samza sql on your local machine

Below are some of the sql queries that you can execute using the samza-sql-console tool from Samza tools package.

This command just prints out all the events in the Kafka topic ProfileChangeStream into console output as a json serialized payload.

```bash
> ./scripts/samza-sql-console.sh --sql "insert into log.consoleoutput select * from kafka.ProfileChangeStream"
```

This command prints out the fields that are selected into the console output as a json serialized payload.

```bash
> ./scripts/samza-sql-console.sh --sql "insert into log.consoleoutput select Name, OldCompany, NewCompany from kafka.ProfileChangeStream"
```


This command showcases the RegexMatch udf and filtering capabilities.

```bash
> ./scripts/samza-sql-console.sh --sql "insert into log.consoleoutput select Name as __key__, Name, NewCompany, RegexMatch('.*soft', OldCompany) from kafka.ProfileChangeStream where NewCompany = 'LinkedIn'"
```

Note: Samza sql console right now doesn’t support queries that need state, for e.g. Stream-Table join, GroupBy and Stream-Stream joins.




# Running Samza SQL on YARN
The [hello-samza](https://github.com/apache/samza-hello-samza) project is an example project designed to help you run your first Samza application. It has examples of applications using the low level task API, high level API as well as Samza SQL.

This tutorial demonstrates a simple Samza application that uses SQL to perform stream processing.

## Get the hello-samza code and start the grid
Please follow the instructions from hello-samza-high-level-yarn on how to build the hello-samza repository and start the yarn grid.

## Create the topic and generate Kafka events
Please follow the steps in the section “Create ProfileChangeStream Kafka topic” and “Generate events into ProfileChangeStream topic” above.

Build a Samza Application package
Before you can run a Samza application, you need to build a package for it. Please follow the instructions from hello-samza-high-level-yarn on how to build the hello-samza application package.

## Run a Samza Application
After you’ve built your Samza package, you can start the app on the grid using the run-app.sh script.

```bash
> ./deploy/samza/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/page-view-filter-sql.properties
```

The app executes the following SQL command :


```sql
insert into kafka.NewLinkedInEmployees select Name from ProfileChangeStream where NewCompany = 'LinkedIn'
```


This SQL performs the following:

* Consumes the Kafka topic ProfileChangeStream which contains the avro serialized ProfileChangeEvent(s)
* Deserializes the events and filters out only the profile change events where NewCompany = ‘LinkedIn’ i.e. Members who have moved to LinkedIn.
* Writes the Avro serialized event that contains the Id and Name of those profiles to Kafka topic NewLinkedInEmployees.

Give the job a minute to startup, and then tail the Kafka topic:

```bash
> ./deploy/kafka/bin/kafka-console-consumer.sh  --zookeeper localhost:2181 --topic NewLinkedInEmployees
```

Congratulations! You’ve now setup a local grid that includes YARN, Kafka, and ZooKeeper, and run a Samza SQL application on it.
## Shutdown and cleanup
To shutdown the app, use the same run-app.sh script with an extra –operation=kill argument

```bash
> ./deploy/samza/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/page-view-filter-sql.properties --operation=kill
```

Please follow the instructions from Hello Samza High Level API - YARN Deployment on how to shutdown and cleanup the app.


# SQL Grammar
Following BNF grammar is based on Apache Calicite’s SQL parser. It is the subset of the capabilities that Calcite supports.  


```
statement:
  |   insert
  |   query 
  
query:
  values
  | {
      select
    }
 
insert:
      ( INSERT | UPSERT ) INTO tablePrimary
      [ '(' column [, column ]* ')' ]
      query 

select:
  SELECT
  { * | projectItem [, projectItem ]* }
  FROM tableExpression
  [ WHERE booleanExpression ]
  [ GROUP BY { groupItem [, groupItem ]* } ]
   
projectItem:
  expression [ [ AS ] columnAlias ]
  | tableAlias . *
 
tableExpression:
  tableReference [, tableReference ]*
  | tableExpression [ NATURAL ] [ LEFT | RIGHT | FULL ] JOIN tableExpression [ joinCondition ]
 
joinCondition:
  ON booleanExpression
  | USING '(' column [, column ]* ')'
 
tableReference:
  tablePrimary
  [ [ AS ] alias [ '(' columnAlias [, columnAlias ]* ')' ] ]
 
tablePrimary:
  [ TABLE ] [ [ catalogName . ] schemaName . ] tableName
   
values:
  VALUES expression [, expression ]*

```
