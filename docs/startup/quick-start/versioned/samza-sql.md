---
layout: page
title: Samza SQL Quick Start
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


### Overview
Samza SQL allows you to define your stream processing logic declaratively as a a SQL query.
This allows you to create streaming pipelines without Java code or configuration unless you 
require user-defined functions ([UDF](#how-to-write-a-udf)). 

You can run Samza SQL locally on your machine or on a YARN cluster.

### Running Samza SQL on your local machine
The [Samza SQL console](https://samza.apache.org/learn/tutorials/0.14/samza-tools.html) allows you to experiment with Samza SQL locally on your machine. 

#### Setup Kafka
Follow the instructions from the [Kafka quickstart](http://kafka.apache.org/quickstart) to start the zookeeper and Kafka server.

Let us create a Kafka topic named “ProfileChangeStream” for this demo.

```bash
./deploy/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic ProfileChangeStream
```

Download the Samza tools package from [here](https://samza.apache.org/learn/tutorials/0.14/samza-tools.html) and use the `generate-kafka-events` script populate the stream with sample data.

```bash
cd samza-tools-<version>
./scripts/generate-kafka-events.sh -t ProfileChangeStream -e ProfileChange
```

#### Using the Samza SQL Console


The simplest SQL query is to read all events from a Kafka topic `ProfileChangeStream` and print them to the console.

```bash
./scripts/samza-sql-console.sh --sql "insert into log.consoleoutput select * from kafka.ProfileChangeStream"
```

Next, let us project a few fields from the input stream.

```bash
./scripts/samza-sql-console.sh --sql "insert into log.consoleoutput select Name, OldCompany, NewCompany from kafka.ProfileChangeStream"
```

You can also filter messages in the input stream based on some predicate. In this example, we filter profiles currently working at LinkedIn, whose previous employer matches the regex `.*soft`. The function `RegexMatch(regex, company)` is an example of 
a UDF that defines a predicate. 

```bash
./scripts/samza-sql-console.sh --sql "insert into log.consoleoutput select Name as __key__, Name, NewCompany, RegexMatch('.*soft', OldCompany) from kafka.ProfileChangeStream where NewCompany = 'LinkedIn'"
```


### Running Samza SQL on YARN
The [hello-samza](https://github.com/apache/samza-hello-samza) project has examples to 
get started with Samza on YARN. You can define your SQL query in a 
configuration file and submit it to a YARN cluster.


```bash
./deploy/samza/bin/run-app.sh --config job.config.loader.factory=org.apache.samza.config.factories.PropertiesConfigLoaderFactory --config job.config.loader.properties.path=$PWD/deploy/samza/config/page-view-filter-sql.properties
```

 
### How to write a UDF
 
 Right now Samza SQL support Scalar UDFs which means that each 
 UDF should act on each record at a time and return the result 
 corresponding to the record. In essence it exhibits the behavior
  of 1 output to an input. Users need to implement the following 
  interface to create a UDF.
  
{% highlight java %}

 /**
  * The base class for the Scalar UDFs. All the scalar UDF classes needs to extend this and implement a method named
  * "execute". The number of arguments for the execute method in the UDF class should match the number of fields
  * used while invoking this UDF in SQL statement.
  * Say for e.g. User creates a UDF class with signature int execute(int var1, String var2). It can be used in a SQL query
  *     select myudf(id, name) from profile
  * In the above query, Profile should contain fields named 'id' of INTEGER/NUMBER type and 'name' of type VARCHAR/CHARACTER
  */
 public interface ScalarUdf {
   /**
    * Udfs can implement this method to perform any initialization that they may need.
    * @param udfConfig Config specific to the udf.
    */
   void init(Config udfConfig);
  
   /**
    * Actual implementation of the udf function
    * @param args
    *   list of all arguments that the udf needs
    * @return
    *   Return value from the scalar udf.
    */
   Object execute(Object... args);
 }
 
{% endhighlight %}

