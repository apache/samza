Validate that all Samza source files have proper license information in their header.

    ./gradlew check

Auto-generate all missing headers in files:

    ./gradlew licenseFormatMain

To release to a local Maven repository:

    ./gradlew clean publishToMavenLocal
    ./gradlew -PscalaVersion=2.8.1 clean publishToMavenLocal

To generate test coverage reports:

    ./gradlew clean jacocoTestReport
