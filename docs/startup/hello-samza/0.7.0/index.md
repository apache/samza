---
layout: page
title: Hello Samza
---

<!-- TODO remove samza code warning when we get our git repo setup -->

*NOTE: The Samza code is not available until we get an Apache git repository. Follow along [here](https://issues.apache.org/jira/browse/INFRA-6617).*

<!-- TODO Make github project public when code is up. -->

The [hello-samza](http://github.com/linkedin/hello-samza) project is a stand-alone project designed to help you run your first Samza job.

### Get the Code

Start by checking out the hello-samza project.

    git clone git://github.com/linkedin/hello-samza.git

This project contains everything you'll need to run your first Samza jobs.

### Start a Grid

A Samza grid usually comprises three different systems: [YARN](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html), [Kafka](http://kafka.apache.org/), and [ZooKeeper](http://zookeeper.apache.org/). The hello-samza project comes with a script called "grid" to help you setup these systems. Start by running:

    bin/grid

This command will download, install, and start ZooKeeper, Kafka, and YARN. All package files will be put in a sub-directory called "deploy" inside hello-samza's root folder.

Once the grid command completes, you can verify that YARN is up and running by going to [http://localhost:8088](http://localhost:8088). This is the YARN UI.

### Build a Samza Job Package

Before you can run a Samza job, you need to build a package for it. This package is what YARN uses to deploy your jobs on the grid.

    mvn clean package
    mkdir -p deploy/samza
    tar -xvf ./samza-job-package/target/samza-job-package-0.7.0-dist.tar.gz -C deploy/samza

### Run a Samza Job

After you've built your Samza package, you can start a job on the grid using the run-job.sh script.

    deploy/samza/bin/run-job.sh --config-factory=samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/wikipedia-feed.properties

The job will consume a feed of real-time edits from Wikipedia, and produce them to a Kafka topic called "wikipedia-raw". Give the job a minute to startup, and then tail the Kafka topic:

    deploy/kafka/bin/kafka-console-consumer.sh  --zookeeper localhost:2181 --topic wikipedia-raw

Pretty neat, right? Now, check out the YARN UI again ([http://localhost:8088](http://localhost:8088)). This time around, you'll see your Samza job is running!

### Generate Wikipedia Statistics

Let's calculate some statistics based on the messages in the wikipedia-raw topic. Start two more jobs:

    deploy/samza/bin/run-job.sh --config-factory=samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/wikipedia-parser.properties
    deploy/samza/bin/run-job.sh --config-factory=samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/wikipedia-stats.properties

The first job (wikipedia-parser) parses the messages in wikipedia-raw, and extracts information about the size of the edit, who made the change, etc. You can take a look at its output with:

    deploy/kafka/bin/kafka-console-consumer.sh  --zookeeper localhost:2181 --topic wikipedia-edits

The last job (wikipedia-stats) reads messages from the wikipedia-edits topic, and calculates counts, every ten seconds, for all edits that were made during that window. It outputs these counts to the wikipedia-stats topic.

    deploy/kafka/bin/kafka-console-consumer.sh  --zookeeper localhost:2181 --topic wikipedia-stats

The messages in the stats topic look like this:

    {"is-talk":2,"bytes-added":5276,"edits":13,"unique-titles":13}
    {"is-bot-edit":1,"is-talk":3,"bytes-added":4211,"edits":30,"unique-titles":30,"is-unpatrolled":1,"is-new":2,"is-minor":7}
    {"bytes-added":3180,"edits":19,"unique-titles":19,"is-unpatrolled":1,"is-new":1,"is-minor":3}
    {"bytes-added":2218,"edits":18,"unique-titles":18,"is-unpatrolled":2,"is-new":2,"is-minor":3}

If you check the YARN UI, again, you'll see that all three jobs are now listed.

### Shutdown

After you're done, you can clean everything up using the same grid script.

    bin/grid stop yarn
    bin/grid stop kafka
    bin/grid stop zookeeper

Congratulations! You've now setup a local grid that includes YARN, Kafka, and ZooKeeper, and run a Samza job on it. Next up, check out the [Background](/learn/documentation/0.7.0/introduction/background.html) and [API Overview](/learn/documentation/0.7.0/api/overview.html) pages.
