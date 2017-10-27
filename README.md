hello-samza
===========

**Hello Samza** is a starter project for [Apache Samza](http://samza.apache.org/) jobs.

### About

[Hello Samza](http://samza.apache.org/startup/hello-samza/0.13/) is developed as part of the [Apache Samza](http://samza.apache.org) project. Please direct questions, improvements and bug fixes there. Questions about [Hello Samza](http://samza.apache.org/startup/hello-samza/0.13/) are welcome on the [dev list](http://samza.apache.org/community/mailing-lists.html) and the [Samza JIRA](https://issues.apache.org/jira/browse/SAMZA) has a hello-samza component for filing tickets.

### Instructions

The **Hello Samza** project contains example Samza applications of high-level API as well as low-level API. The following are the instructions to install the binaries and run the applications in a local Yarn cluster. See also [Hello Samza](http://samza.apache.org/startup/hello-samza/0.13/) and [Hello Samza High Level API](http://samza.apache.org/learn/tutorials/latest/hello-samza-high-level-yarn.html) for more information.

#### 1. Get the Code

Check out the hello-samza project:

```
git clone https://git.apache.org/samza-hello-samza.git hello-samza
cd hello-samza
git checkout latest
```

This project contains everything you'll need to run your first Samza application.

#### 2. Start a Grid

A Samza grid usually comprises three different systems: [YARN](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html), [Kafka](http://kafka.apache.org/), and [ZooKeeper](http://zookeeper.apache.org/). The hello-samza project comes with a script called "grid" to help you setup these systems. Start by running:

```
./bin/grid bootstrap
```

This command will download, install, and start ZooKeeper, Kafka, and YARN. It will also check out the latest version of Samza and build it. All package files will be put in a sub-directory called "deploy" inside hello-samza's root folder.

If you get a complaint that _JAVA_HOME_ is not set, then you'll need to set it to the path where Java is installed on your system.

Once the grid command completes, you can verify that YARN is up and running by going to [http://localhost:8088](http://localhost:8088). This is the YARN UI.

#### 3. Build a Samza Application Package

Before you can run a Samza application, you need to build a package for it. This package is what YARN uses to deploy your apps on the grid. Use the following command in hello-samza project to build and deploy the example applications:

```
./bin/deploy.sh
```

#### 4. Run a Samza Application

After you've built your Samza package, you can start the example applications on the grid.

##### - High-level API Examples

Package [samza.examples.cookbook](https://github.com/apache/samza-hello-samza/tree/master/src/main/java/samza/examples/cookbook) contains various examples of high-level API operator usage, such as map, partitionBy, window and join. Each example is a runnable Samza application with the steps in the class javadocs, e.g [PageViewAdClickJoiner](https://github.com/apache/samza-hello-samza/blob/master/src/main/java/samza/examples/cookbook/PageViewAdClickJoiner.java).

Package [samza.examples.wikipedia.application](https://github.com/apache/samza-hello-samza/tree/master/src/main/java/samza/examples/wikipedia/application) contains a small Samza application which consumes the real-time feeds from Wikipedia, extracts the metadata of the events, and calculates statistics of all edits in a 10-second window. You can start the app on the grid using the run-app.sh script:

```
./deploy/samza/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/wikipedia-application.properties
```

Once the job is started, we can tail the kafka topic by:

```
./deploy/kafka/bin/kafka-console-consumer.sh  --zookeeper localhost:2181 --topic wikipedia-stats
```

A code walkthrough of this application can be found [here](http://samza.apache.org/learn/tutorials/latest/hello-samza-high-level-code.html).

##### - Low-level API Examples

Package [samza.examples.wikipedia.task](https://github.com/apache/samza-hello-samza/tree/master/src/main/java/samza/examples/wikipedia/task) contains the low-level API Samza code for the Wikipedia example. To run it, use the following scripts:

```
deploy/samza/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/wikipedia-feed.properties
deploy/samza/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/wikipedia-parser.properties
deploy/samza/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/wikipedia-stats.properties
```

Once the jobs are started, you can use the same _kafka-console-consumer.sh_ command as in the high-level API Wikipedia example to check out the output of the statistics.

### Contribution

To start contributing on [Hello Samza](http://samza.apache.org/startup/hello-samza/0.13/) first read [Rules](http://samza.apache.org/contribute/rules.html) and [Contributor Corner](https://cwiki.apache.org/confluence/display/SAMZA/Contributor%27s+Corner). Notice that [Hello Samza](http://samza.apache.org/startup/hello-samza/0.13/) git repository does not support git pull request.
