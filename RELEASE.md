Validate that all Samza source files have proper license information in their header.

    ./gradlew check

Auto-generate all missing headers in files:

    ./gradlew licenseFormatMain

To release to a local Maven repository:

    ./gradlew clean publishToMavenLocal
    ./gradlew -PscalaVersion=2.9.2 clean publishToMavenLocal

To build a tarball suitable for an ASF source release (and its accompanying MD5 file):

First, clean any non-checked-in files from git (this removes all such files without prompting):

    git clean -d -f

Then build the tarball:

    ./gradlew clean sourceRelease
