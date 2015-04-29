
To use gradle to build/run the hello-samza project:

1) the project is configured to download and use gradle version 2.3 - on first task execution, it will download the required gradle jars.

2) download/install yarn/kafka/zookeeper:

	$ ./gradlew installGrid

3) build hello-samza job package:

	$ ./gradlew distTar

4) deploy hello-samza project to grid:

	$ ./gradlew deployHelloSamza

5) start the grid (starts up yarn/kafka/zookeeper):

	$ ./gradlew startGrid

6) run the various Samza tasks that are part of hello-samza project:

	$ ./gradlew runWikiFeed
	$ ./gradlew runWikiParser
	$ ./gradlew runWikiStats

7) view all the current Kafka topics:

	$ ./gradlew listKafkaTopics

8) view the Kafka topics output by the various Samza tasks:

	$ ./gradlew dumpWikiRaw
	( output of Kafka topic scrolls by)
	CTRL-c

	$ ./gradlew dumpWikiEdits
	( output of Kafka topic scrolls by)
	CTRL-c

	$ ./gradlew dumpWikiStats
	( output of Kafka topic scrolls by)
	CTRL-c

9) stop all the components:

	$ ./gradlew stopGrid

Shortcut: using the 'runWiki*' tasks directly will do steps 3-6 automatically.

