# Samza Release Procedure

Releasing Samza involves the following steps:

* Send a [DISCUSS] to dev@samza.apache.org. [Example](http://mail-archives.apache.org/mod_mbox/samza-dev/201503.mbox/%3CCABYbY7dsYAQo4_6qBvmUSOF37%2BUfsHRQ3dKOJV1qHJUTetKdAA%40mail.gmail.com%3E)
* Create the Release Candidate
* Send a [VOTE] to dev@samza.apache.org. [Example](http://mail-archives.apache.org/mod_mbox/samza-dev/201503.mbox/%3CCAOErhNQsehZ8iEXsP5saKgr9qjD%3DART7-2OCWJcCbXJko9FV4A%40mail.gmail.com%3E)
* Wait till the [VOTE] completes and send [RESULT][VOTE]. [Example](http://mail-archives.apache.org/mod_mbox/samza-dev/201412.mbox/%3CCADiKvVuAkgiR7-0VBYccez96xtfV6edavdy7xc%3Drg9GCftaEsg%40mail.gmail.com%3E)
* Publish source tarball to Apache SVN
* Publish website documents for new release
* Write a blog post on [Apache Blog](https://blogs.apache.org/samza/)
* Update the Samza version of the master branch to the next version
* Update [samza-hello-samza](https://github.com/apache/samza-hello-samza) to use the new Samza version

The following sections will be focusing on creating the release candidate, publish the source tarball, and publish website documents.

## Steps to release Samza binary artifacts

Before you start, here are a few prerequisite steps that would be useful later:

   * You may find https://www.apache.org/dev/release-signing.html useful for providing some additional context regarding the release process.
   * Make sure you have your GPG key generated/signed/published and added to KEYS file. GPG tools: https://gpgtools.org/
   * Setup your personal website on Apache: https://www.apache.org/dev/new-committers-guide.html#personal-web-space
   * Setup access to author the apache blog: https://infra.apache.org/pages/project-blog.html

And before you proceed, do the following steps:

   * Create a branch $VERSION from the latest master branch.
   * Checkout the $VERSION branch.
   * Update the gradle.properties s.t. the following property is $VERSION w/o the suffix '-SNAPSHOT':
      version=$VERSION
   * Change the samza_executable variable in samza-test/src/main/python/configs/tests.json to $VERSION w/o the suffix '-SNAPSHOT'.
   * Change the samza-test versions in samza-test/src/main/config/join/README to $VERSION w/o the suffix '-SNAPSHOT'.
   * Change the executable version in samza-test/src/main/python/stream_processor.py to $VERSION w/o the suffix '-SNAPSHOT'.
   * Push the changes to the $VERSION branch

Validate Samza using all our supported build matrix.

```bash
    ./bin/check-all.sh
```

Run integration tests for YARN and standalone

```bash
    ./bin/integration-tests.sh . yarn-integration-tests
    ./bin/integration-tests.sh . standalone-integration-tests
```

To release to a local Maven repository:

```bash
    ./gradlew clean publishToMavenLocal
```

To build a tarball suitable for an ASF source release (and its accompanying md5 file):

First, clean any non-checked-in files from git (this removes all such files without prompting):

```bash
    git clean -fdx
```

Alternatively, you can make a fresh clone of the repository to a separate directory:

```bash
    git clone http://git-wip-us.apache.org/repos/asf/samza.git samza-release
    cd samza-release
    git checkout $VERSION
```

Then build the source and samza-tools tarballs:

```bash
    ./gradlew clean sourceRelease && ./gradlew releaseToolsTarGz
```

Then sign them:

   ```bash
    gpg --sign --armor --detach-sig ./build/distribution/source/apache-samza-$VERSION-src.tgz
    gpg --sign --armor --detach-sig ./samza-tools/build/distributions/samza-tools_$SCALAVERSION-$VERSION.tgz
   ```

Create MD5 signatures:

   ```bash
    gpg --print-md MD5 ./build/distribution/source/apache-samza-$VERSION-src.tgz > ./build/distribution/source/apache-samza-$VERSION-src.tgz.md5
    gpg --print-md MD5 ./samza-tools/build/distributions/samza-tools_$SCALAVERSION-$VERSION.tgz > ./samza-tools/build/distributions/samza-tools_$SCALAVERSION-$VERSION.tgz.md5
   ```

Create SHA1 signatures:

   ```bash
    gpg --print-md SHA1 ./build/distribution/source/apache-samza-$VERSION-src.tgz > ./build/distribution/source/apache-samza-$VERSION-src.tgz.sha1
    gpg --print-md SHA1 ./samza-tools/build/distributions/samza-tools_$SCALAVERSION-$VERSION.tgz > ./samza-tools/build/distributions/samza-tools_$SCALAVERSION-$VERSION.tgz.sha1
   ```

Upload the build artifacts to your Apache home directory (you can authorize your public SSH key through https://id.apache.org):

   ```bash
    sftp <apache-username>@home.apache.org
    cd public_html
    mkdir samza-$VERSION-rc0
    cd samza-$VERSION-rc0
    put ./build/distribution/source/apache-samza-$VERSION-src.* .
    put ./samza-tools/build/distributions/samza-tools_$SCALAVERSION-$VERSION.* .
    bye
   ```

Make a signed git tag for the release candidate (you may need to use -u to specify key id):

```bash
    git tag -s release-$VERSION-rc0 -m "Apache Samza $VERSION release candidate 0"
```

Push the release tag to remote repository:

```bash
    git push origin release-$VERSION-rc0
```

Edit `$HOME/.gradle/gradle.properties` and add your GPG key information (without the comments):

   ```bash
     signing.keyId=01234567                          # Your GPG key ID, as 8 hex digits
     signing.secretKeyRingFile=/path/to/secring.gpg  # Normally in $HOME/.gnupg/secring.gpg
     signing.password=YourSuperSecretPassphrase      # Plaintext passphrase to decrypt key
     nexusUsername=yourname                          # Your username on Apache's LDAP
     nexusPassword=password                          # Your password on Apache's LDAP
   ```

Putting your passwords there in plaintext is unfortunately unavoidable. The
[nexus plugin](https://github.com/bmuschko/gradle-nexus-plugin) supports asking
for them interactively, but unfortunately there's a
[Gradle issue](http://issues.gradle.org/browse/GRADLE-2357) which prevents us
from reading keyboard input (because we need `org.gradle.jvmargs` set).

Build binary artifacts and upload them to the staging repository:

   ```bash
    # Set this to the oldest JDK which we are currently supporting for Samza.
    # If it's built with Java 8, the classes won't be readable by Java 7.
    export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_20.jdk/Contents/Home
    ./gradlew clean uploadArchives
   ```

Go to [repository web interface](https://repository.apache.org/), log in with
Apache LDAP credentials, go to "Staging Repositories", select the org.apache.samza
repository just created, and close it. This may take a minute or so. When it
finishes, the UI shows a staging repository URL. This can be used in a project
that depends on Samza, to test the release candidate.

The instructions above publish the Samza artifacts for scala 2.11. To publish for scala 2.12:

* Set the desired `scalaSuffix` in `gradle.properties`.
* Run `./gradlew clean uploadArchives` to generate and upload the Samza artifacts.

# After the VOTE has passed

If the VOTE has successfully passed on the release candidate, you can log in to the  [repository web interface](https://repository.apache.org) (same as above) and "release"  the org.apache.samza repository listed under "Staging Repositories". This may take a minute or so.
This will publish the samza release artifacts to the open source [maven repository](https://repo1.maven.org/maven2).
Update gradle.properties with the next master version (1.0.0 -> 1.1.0 for example).

## Steps to Upload Source Tarball to Apache SVN

Note that only PMCs have permissions to commit to this repository.

Check out the following Apache dist SVN to local:

```bash
   svn checkout https://dist.apache.org/repos/dist/release/samza samza-dist
```

Create the new version's sub-directory and add the source tarball, MD5, and asc files from the
previous step to the new directory:

   ```bash
   cd samza-dist
   mkdir $VERSION
   cp ${SAMZA_SRC_ROOT}/build/distribution/source/apache-samza-$VERSION-src.tgz $VERSION
   cp ${SAMZA_SRC_ROOT}/build/distribution/source/apache-samza-$VERSION-src.tgz.md5 $VERSION
   cp ${SAMZA_SRC_ROOT}/build/distribution/source/apache-samza-$VERSION-src.tgz.asc $VERSION
   svn add $VERSION
   ```

Commit to Apache release SVN

```bash
   svn ci -m "Releasing Apache Samza $VERSION Source Tarballs"
```

Check the download link [here](http://www-us.apache.org/dist/samza/) to make sure that the mirror
site has picked up the new release. The third-party mirrors may take upto 24 hours to pick-up the release.
In order to ensure that the release is available in public mirrors, wait for the release jars
to show up in [maven central](http://search.maven.org/#search%7Cga%7C1%7Csamza). A full list of mirrors can be found [here](http://www.apache.org/dyn/closer.cgi).
Do not publish the website or any public document until the release jars are available for download.

## Steps to Update Public Documentation

Please refer to docs/README.md, specifically "Release-new-version Website Checklist" section.

## Write a blog post on the Apache blog site

You can use the same content as the blog you wrote for the Samza site.

Notes:
* Apache blog editor is primitive. You have to provide HTML formatted document, but you should pretty much be able
to use the HTML source generated for the Samza site.
* Only PMCs have permissions to update https://blogs.apache.org/samza/. If you are not a PMC, then you can ask a PMC to
update the blog or ask a PMC to add write permissions for you.

## Update samza-hello-samza

Make sure that the `master` branch of [samza-hello-samza](https://github.com/apache/samza-hello-samza) is synced with the
`latest` branch.
```bash
   git merge latest
```
In the `master` branch, remove the `-SNAPSHOT` part of the Samza version in `gradle.properties` and `pom.xml`.

In the `latest` branch, update the Samza version in `gradle.properties` and `pom.xml` to be the next version of Samza.
