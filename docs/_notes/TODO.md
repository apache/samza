## TODO items

* rename everything to samza
* cut a 0.7 branch
* release samza 0.7.0 snapshot to artifactory
* docs
  YARN
    Fault tolerance
    Security
  * comparisons pages
    * JMS
    * Aurora
    * JMS
    * S4
  * configuration update (make sure we have everything)
  * add versioning link in docs page, so you can get to other version numbers
* tutorials
  * Configuring a Kafka System
  * Configuring a YARN cluster
  * Joining Streams
  * Sorting a Stream
  * Group-by and Counting
  * Initializing and Closing
  * Windowing
  * Committing
* add versioning link in docs page, so you can get to other version numbers

## Rules/coding guideline/config brainstorm

Before contributing to Samza, please have a look at the rules listed below.

### Coding Style

TODO add coding style rules for Java and Scala. Should look into enforcing them through Maven.

### Metrics

TODO we should add some metric rules here.

### Configuration

12. TODO When in Scala * how to handle null/none?

```
public interface MyConfig
{
  @Config("yarn.package.uri")
  URI getYarnPackageUri();

  @Config("systems.<system>.consumer.factory")
  @Default()
  Class<StreamConsumerFactory> getSystemsConsumerFactory(String systemName);

  @Config("systems.<system>.*")
  Map<String, String> getSystems(String systemName);
}
```

<!-- https://github.com/brianm/config-magic -->

Open:

* We are mixing wiring and configuration together. How do other systems handle this?
* We have fragmented configuration (anybody can define configuration). How do other systems handle this?
* Want to do "best practices" configuration name validation at compile time (enforce rules outlined above).
* How to handle getting of submaps? Should it return a config object, instead? If so, should it be typed? (e.g. config.getSubset("foo.bar.") = Config or config.getSubset("foo.bar.") = SystemConfig)

Solved:

* Want to auto-generate documentation based off of configuration.
  @Description
* Should support global defaults for a config property. Right now, we do config.getFoo.getOrElse() everywhere.
  @Default
* How to handle undefined configuration? How to make this interoperable with both Java and Scala (i.e. should we support Option in Scala)? 
  getInt("foo", null) or getInt("foo"); latter throws an exception if undefined. In Scala, Option(getInt("foo", null)) does the trick.
* Should work with CFG2 and Java .properties files.
* Should remain immutable.
* Should remove implicits. Just confusing.
  val kafkaConfig = new KafkaConfig(config)
  val systemConfig = new SystemConfig(config)

```
public interface MyConfig
{
  @Config("yarn.package.uri")
  URI getYarnPackageUri();

  @Config("systems.<system>.consumer.factory")
  @Default()
  Class<StreamConsumerFactory> getSystemsConsumerFactory(String systemName);

  @Config("systems.<system>.*")
  Map<String, String> getSystems(String systemName);
}
```
