# Samza Release Procedure

Releasing Samza involves the following steps:

* Send a [DISCUSS] to dev@samza.apache.org. [Example](http://mail-archives.apache.org/mod_mbox/samza-dev/201503.mbox/%3CCABYbY7dsYAQo4_6qBvmUSOF37%2BUfsHRQ3dKOJV1qHJUTetKdAA%40mail.gmail.com%3E)
* Create the Release Candidate
* Send a [VOTE] to dev@samza.apache.org. [Example](http://mail-archives.apache.org/mod_mbox/samza-dev/201503.mbox/%3CCAOErhNQsehZ8iEXsP5saKgr9qjD%3DART7-2OCWJcCbXJko9FV4A%40mail.gmail.com%3E)
* Wait till the [VOTE] completes and send [RESULT][VOTE]. [Example](http://mail-archives.apache.org/mod_mbox/samza-dev/201412.mbox/%3CCADiKvVuAkgiR7-0VBYccez96xtfV6edavdy7xc%3Drg9GCftaEsg%40mail.gmail.com%3E)
* Publish source tarball to Apache SVN
* Publish website documents for new release
* Write a blog post on [Apache Blog](https://blogs.apache.org/samza/) (Note: Apache blog editor is primitive. You have to provide HTML formatted document.)

The following sections will be focusing on creating the release candidate, publish the source tarball, and publish website documents.

## Steps to release Samza binary artifacts

Before you start, here are a few prerequisite steps that would be useful later:

   * Make sure you have your GPG key generated and added to KEYS file. GPG tools: https://gpgtools.org/
   * Setup your personal website on Apache: http://www.apache.org/dev/new-committers-guide.html
   * Setup access to author the apache blog: http://www.apache.org/dev/project-blogs#grantrights

And before you proceed, do the following steps:

   * create a branch $VERSION from the latest master branch
   * checkout the $VERSION branch
   * update the gradle.properties s.t. the following property is $VERSION w/o the suffix '-SNAPSHOT':
      version=$VERSION
   * change the samza_executable variable in samza-test/src/main/python/configs/tests.json to $VERSION w/o the suffix '-SNAPSHOT'. 
   * change the samza-test versions in samza-test/src/main/config/join/README to $VERSION w/o the suffix '-SNAPSHOT'.
   * change the executable version in samza-test/src/main/python/stream_processor.py to $VERSION w/o the suffix '-SNAPSHOT'.
   * push the changes to the $VERSION branch

Validate Samza using all our supported build matrix.

    ./bin/check-all.sh

To release to a local Maven repository:

    ./gradlew clean publishToMavenLocal

To build a tarball suitable for an ASF source release (and its accompanying MD5 file):

First, clean any non-checked-in files from git (this removes all such files without prompting):

    git clean -fdx

Alternatively, you can make a fresh clone of the repository to a separate directory:

    git clone http://git-wip-us.apache.org/repos/asf/samza.git samza-release
    cd samza-release

Then build the source and samza-tools tarballs:

    ./gradlew clean sourceRelease && ./gradlew releaseToolsTarGz

Then sign them:

    gpg --sign --armor --detach-sig ./build/distribution/source/apache-samza-*.tgz
    gpg --sign --armor --detach-sig ./samza-tools/build/distributions/samza-tools-*.tgz

Create MD5 signatures:

    gpg --print-md MD5 ./build/distribution/source/apache-samza-*.tgz > ./build/distribution/source/apache-samza-*.tgz.md5
    gpg --print-md MD5 ./samza-tools/build/distributions/samza-tools-*.tgz > ./samza-tools/build/distributions/samza-tools-*.tgz.md5

Create SHA1 signatures:

    gpg --print-md SHA1 ./build/distribution/source/apache-samza-*.tgz > ./build/distribution/source/apache-samza-*.tgz.sha1
    gpg --print-md SHA1 ./samza-tools/build/distributions/samza-tools-*.tgz > ./samza-tools/build/distributions/samza-tools-*.tgz.sha1

Upload the build artifacts to your Apache home directory:

    sftp <apache-username>@home.apache.org
    cd public_html
    mkdir samza-$VERSION-rc0
    cd samza-$VERSION-rc0
    put ./build/distribution/source/apache-samza-$VERSION-src.* .
    put ./samza-tools/build/distributions/samza-tools-$VERSION.* .
    bye

Make a signed git tag for the release candidate:

    git tag -s release-$VERSION-rc0 -m "Apache Samza $VERSION release candidate 0"

Push the release tag to remote repository:

    git push origin release-$VERSION-rc0

Edit `$HOME/.gradle/gradle.properties` and add your GPG key information (without the comments):

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
    export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_20.jdk/Contents/Home
    ./gradlew clean uploadArchives

Go to [repository web interface](https://repository.apache.org/), log in with
Apache LDAP credentials, go to "Staging Repositories", select the org.apache.samza
repository just created, and close it. This may take a minute or so. When it
finishes, the UI shows a staging repository URL. This can be used in a project
that depends on Samza, to test the release candidate.


If the VOTE has successfully passed on the release candidate, you can log in to the 
[repository web interface](https://repository.apache.org) (same as above) and "release" 
the org.apache.samza repository listed under "Staging Repositories".

The instructions above publish the Samza artifacts for scala 2.11. To publish for scala 2.10 and 2.12:

* Set the desired `scalaSuffix` in `gradle.properties`.
* Run `./gradlew clean uploadArchives` to generate and upload the Samza artifacts.
* Login to the [repository web interface](https://repository.apache.org/) with your Apache LDAP 
credentials, "close" the created repository and "release" it.

## Steps to Upload Source Tarball to Apache SVN

Check out the following Apache dist SVN to local:

   svn checkout https://dist.apache.org/repos/dist/release/samza samza-dist

Create the new version's sub-directory and add the source tarball, MD5, and asc files from the 
previous step to the new directory:

   cd samza-dist
   mkdir $VERSION
   cp ${SAMZA_SRC_ROOT}/build/distribution/source/apache-samza-$VERSION-src.tgz $VERSION
   cp ${SAMZA_SRC_ROOT}/build/distribution/source/apache-samza-$VERSION-src.tgz.MD5 $VERSION
   cp ${SAMZA_SRC_ROOT}/build/distribution/source/apache-samza-$VERSION-src.tgz.asc $VERSION
   svn add $VERSION

Commit to Apache release SVN

   svn ci -m "Releasing Apache Samza $VERSION Source Tarballs"

Check the download link [here](http://www-us.apache.org/dist/samza/) to make sure that the mirror
site has picked up the new release. The third-party mirrors may take upto 24 hours to pick-up the release. 
In order to ensure that the release is available in public mirrors, wait for the release jars 
to show up in [maven central](http://search.maven.org/#search%7Cga%7C1%7Csamza). A full list of mirrors can be found [here](http://www.apache.org/dyn/closer.cgi).
Do not publish the website or any public document until the release jars are available for download.

## Steps to Update Public Documentation

Please refer to docs/README.md, specifically "Release-new-version Website Checklist" section. 
