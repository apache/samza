---
layout: page
title: Coding Guide
---

<!-- TODO link to hudson when we have an apache hudson boxes. -->

These guidelines are meant to encourage consistency and best practices amongst people working on the Samza code base. They should be observed unless there is a compelling reason to ignore them.

### Basic Stuff

* Avoid cryptic abbreviations. Single letter variable names are fine in very short methods with few variables, otherwise make them informative.
* Clear code is preferable to comments. When possible make your naming so good you don't need comments. When that isn't possible comments should be thought of as mandatory, write them to be read.
* Logging, configuration, and public APIs are our "UI". Make them pretty, consistent, and usable.
* There is not a maximum line length (certainly not 80 characters, we don't work on punch cards any more), but be reasonable.
* Don't be sloppy. Don't check in commented out code: we use version control, it is still there in the history. Don't leave TODOs in the code or FIXMEs if you can help it. Don't leave println statements in the code. Hopefully this is all obvious.
* We want people to use our stuff, which means we need clear, correct documentation. User documentation should be considered a part of any user-facing the feature, just like unit tests or performance results.
* Don't duplicate code (duh).
* Any API that's user-facing (something that a Samza job could use) should be defined in samza-api as a Java interface. Scala is for implementation only.

### Scala

We are following the style guide given here (though not perfectly). Below are some specifics worth noting:

* Scala is a very flexible language. Use restraint. Magic cryptic one-liners do not impress us, readability impresses us.
* Use vals when possible.
* Use private when possible for member variables.
* Method and member variable names should be in camel case with an initial lower case character like aMethodName.
* Constants should be camel case with an initial capital LikeThis not LIKE_THIS.
* Prefer a single top-level class per file for ease of finding things.
* Do not use semi-colons unless required.
* Avoid getters and setters - stick to plain vals or vars instead. If (later on) you require a custom setter (or getter) for a var named myVar then add a shadow var myVar\_underlying and override the setter (def myVar =) and the getter (def myVar = myVar\_underlying).
* Perfer Option to null in scala APIs.
* Use named arguments when passing in literal values if the meaning is at all unclear, for example instead of Utils.delete(true) prefer Utils.delete(recursive=true).
* Indentation is 2 spaces and never tabs. One could argue the right amount of indentation, but 2 seems to be standard for Scala and consistency is best here since there is clearly no "right" way.
* Include the optional parenthesis on a no-arg method only if the method has a side-effect, otherwise omit them. For example fileChannel.force() and fileChannel.size. This helps emphasize that you are calling the method for the side effect, which is changing some state, not just getting the return value.
* Prefer case classes to tuples in important APIs to make it clear what the intended contents are.

### Logging

* We use [grizzled-slf4j](http://software.clapper.org/grizzled-slf4j/) for Scala logging, and [SLF4J](http://www.slf4j.org/) (with [Log4J](http://logging.apache.org/log4j/2.x/)) for Java.
* Logging is one third of our "UI" and it should be taken seriously. Please take the time to assess the logs when making a change to ensure that the important things are getting logged and there is no junk there.
* Don't include a stack trace in INFO, or above, unless there is really something wrong. Stack traces in logs should signal something is wrong, not be informative. If you want to be informative, write an actual log line that say's what's important, and save the stack trace for DEBUG.
* Logging statements should be complete sentences with proper capitalization that are written to be read by a person not necessarily familiar with the source code. * It is fine to put in hacky little logging statements when debugging, but either clean them up or remove them before checking in. So logging something like "INFO: entering SyncProducer send()" is not appropriate.
* Logging should not mention class names or internal variables.
* There are six levels of logging TRACE, DEBUG, INFO, WARN, ERROR, and FATAL, they should be used as follows.
  * INFO is the level you should assume the software will be run in. INFO messages are things which are not bad but which the user will definitely want to know about every time they occur.
  * TRACE and DEBUG are both things you turn on when something is wrong and you want to figure out what is going on. DEBUG should not be so fine grained that it will seriously effect the performance of the Samza job. TRACE can be anything.
  * WARN and ERROR indicate something that is bad. Use WARN if you aren't totally sure it is bad, and ERROR if you are.
  * Use FATAL only right before calling System.exit().

### Metrics

* Metrics should be taken seriously. The goal with metrics is to provide enough information that users can determine that their Samza job is running properly.
* We have a metrics package in samza-api. It supports counters and gauges, and should be used for all features.
* Any new features should come with appropriate metrics to know the feature is working correctly. This is at least as important as unit tests as it verifies production.
* Metric naming should be of the form: group=samza.core.task.runner, name=UnprocessedMessages.

### Unit Tests

* New patches should come with unit tests that verify the functionality being added.
* Unit tests are first rate code, and should be treated like it. They should not contain code duplication, cryptic hackery, or anything like that.
* Unit tests should test the least amount of code possible, don't start Kafka or YARN unless there is no other way to test a single class or small group of classes in isolation.
* Tests should not depend on any external resources, they need to set up and tear down their own stuff. This means if you want zookeeper it needs to be started and stopped, you can't depend on it already being there. Likewise if you need a file with some data in it, you need to write it in the beginning of the test and delete it (pass or fail).
* Do not use sleep or other timing assumptions in tests, it is always, always, always wrong and will fail intermittently on any test server with other things going on that causes delays. Write tests in such a way that they are not timing dependent. Seriously. One thing that will help this is to never directly use the system clock in code (i.e. System.currentTimeMillis) but instead to use getTime: () => Long, so that time can be mocked.
* It must be possible to run the tests in parallel, without having them collide. This is a practical thing to allow multiple branches to CI on a single CI server. This means you can't hard code directories or ports or things like that in tests because two instances will step on each other.

### Configuration

* Configuration is the final third of our "UI".
* All configuration names that define time must end with .ms (e.g. foo.bar.ms=1000).
* All configuration names that define a byte size must end with .bytes (e.g. foo.bar.bytes=1000).
* All configuration names that define a URI must end with .uri (e.g. yarn.package.uri).
* All configuration names that support a CSV list must end with .list (e.g. task.input.stream.list)
* All configuration names that define a class must end with .class (e.g. task.command.class).
* All configuration names that define a factory class must end with .factory.class (e.g. systems.kafka.consumer.factory.class).
* Configuration will always be defined as simple key/value pairs (e.g. a=b).
* When configuration is related, it must be grouped using the same prefix (e.g. yarn.container.count=1, yarn.container.memory.bytes=1073741824).
* When configuration must be defined multiple times, the key should be parameterized (e.g. systems.kafka.consumer.factory=x, systems.kestrel.consumer.factory=y). *When such configuration must be referred to, its parameter should be used (e.g. foo.bar.system=kafka, foo.bar.system=kestrel).
* All getter methods must be a camel case match with their configuration names (e.g. yarn.package.uri and getYarnPackageUri).
* Reading configuration should only be done in factories and main methods. Don't pass Config objects around.
* Names should be thought through from the point of view of the person using the config, but often programmers choose configuration names that make sense for someone reading the code.
* Often the value that makes most sense in configuration is not the one most useful to program with. For example, let's say you want to throttle I/O to avoid using up all the I/O bandwidth. The easiest thing to implement is to give a "sleep time" configuration that let's the program sleep after doing I/O to throttle down its rate. But notice how hard it is to correctly use this configuration parameter, the user has to figure out the rate of I/O on the machine, and then do a bunch of arithmetic to calculate the right sleep time to give the desired rate of I/O on the system. It is much, much, much better to just have the user configure the maximum I/O rate they want to allow (say 5MB/sec) and then calculate the appropriate sleep time from that and the actual I/O rate. Another way to say this is that configuration should always be in terms of the quantity that the user knows, not the quantity you want to use.
* Configuration is the answer to problems we can't solve up front for some reason--if there is a way to just choose a best value do that instead.
* Configuration should come from the job-level properties file. No additional sources of config (environment variables, system properties, etc) should be added as these usually inhibit running multiple instances of a broker on one machine.

### Concurrency

* Encapsulate synchronization. That is, locks should be private member variables within a class and only one class or method should need to be examined to verify the correctness of the synchronization strategy.
* There are a number of gotchas with threads and threadpools: is the daemon flag set appropriately for your threads? are your threads being named in a way that will distinguish their purpose in a thread dump? What happens when the number of queued tasks hits the limit (do you drop stuff? do you block?).
* Prefer the java.util.concurrent packages to either low-level wait-notify, custom locking/synchronization, or higher level scala-specific primitives. The util.concurrent stuff is well thought out and actually works correctly. There is a generally feeling that threads and locking are not going to be the concurrency primitives of the future because of a variety of well-known weaknesses they have. This is probably true, but they have the advantage of actually being mature enough to use for high-performance software right now; their well-known deficiencies are easily worked around by equally well known best-practices. So avoid actors, software transactional memory, tuple spaces, or anything else not written by Doug Lea and used by at least a million other productions systems. :-)

### Backwards Compatibility

* Samza uses [Semantic Versioning](http://semver.org/).
* Backwards incompatible API changes, config changes, or library upgrades should only happen between major revision changes, or when the major revision is 0.
* The samza-api and samza-core packages should never depend on anything except logging and JSON (jackson) libraries. Prefer granular packages that isolate dependencies, rather than larger packages that group un-related dependencies together.
