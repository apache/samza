Validate that all Samza source files have proper license information in their header.

    ./gradlew check

Auto-generate all missing headers in files:

    ./gradlew licenseFormatMain

To release to a local Maven repository:

    ./gradlew clean publishToMavenLocal
    ./gradlew -PscalaVersion=2.9.2 clean publishToMavenLocal

To build a tarball suitable for an ASF source release (and its accompanying MD5 file):

First, clean any non-checked-in files from git (this removes all such files without prompting):

    git clean -fdx

Alternatively, you can make a fresh clone of the repository to a separate directory:

    git clone https://git-wip-us.apache.org/repos/asf/incubator-samza.git samza-release
    cd samza-release

Then build the tarball:

    ./gradlew clean sourceRelease

Then sign it:

    gpg --sign --armor --detach-sig build/distribution/source/samza-sources-*.tgz

Make a signed git tag for the release candidate:

    git tag -s release-$VERSION-rc0 -m "Apache Samza $VERSION release candidate 0"

Edit `$HOME/.gradle/gradle.properties` and add your GPG key information:

    signing.keyId=01234567                          # Your GPG key ID, as 8 hex digits
    signing.secretKeyRingFile=/path/to/secring.gpg  # Normally in $HOME/.gnupg/secring.gpg
    signing.password=YourSuperSecretPassphrase      # Plaintext passphrase to decrypt key
    nexusUsername=yourname                          # Your username on Apache's LDAP
    nexusPassword=password                          # Your password on Apache's LDAP

Putting your passwords there in plaintext is unfortunately unavoidable. The
[nexus plugin](https://github.com/bmuschko/gradle-nexus-plugin) supports asking
for them interactively, but unfortunately there's a
[Gradle issue](http://issues.gradle.org/browse/GRADLE-2357) which prevents us
from reading keyboard input (because we need `org.gradle.jvmargs` set).

Build binary artifacts and upload them to the staging repository:

    # Set this to the oldest JDK which we are currently supporting for Samza.
    # If it's built with Java 8, the classes won't be readable by Java 7.
    export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home
    ./gradlew clean uploadArchives
    ./gradlew -PscalaVersion=2.9.2 clean uploadArchives

Go to [repository web interface](https://repository.apache.org/), log in with
Apache LDAP credentials, go to "Staging Repositories", select the org.apache.samza
repository just created, and close it. This may take a minute or so. When it
finishes, the UI shows a staging repository URL. This can be used in a project
that depends on Samza, to test the release candidate.
