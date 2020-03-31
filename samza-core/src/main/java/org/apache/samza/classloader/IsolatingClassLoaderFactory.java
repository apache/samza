/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.classloader;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.cytodynamics.matcher.BootstrapClassPredicate;
import com.linkedin.cytodynamics.matcher.GlobMatcher;
import com.linkedin.cytodynamics.nucleus.DelegateRelationship;
import com.linkedin.cytodynamics.nucleus.DelegateRelationshipBuilder;
import com.linkedin.cytodynamics.nucleus.IsolationLevel;
import com.linkedin.cytodynamics.nucleus.LoaderBuilder;
import com.linkedin.cytodynamics.nucleus.OriginRestriction;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Use this to build a classloader for running Samza which isolates the Samza framework code/dependencies from the
 * application code/dependencies.
 */
public class IsolatingClassLoaderFactory {
  private static final Logger LOG = LoggerFactory.getLogger(IsolatingClassLoaderFactory.class);

  private static final String LIB_DIRECTORY = "lib";

  /**
   * Build a classloader which will isolate Samza framework code from application code. Samza framework classes and
   * application-specific classes will be loaded using a different classloaders. This will enable dependencies of each
   * category of classes to also be loaded separately, so that runtime dependency conflicts do not happen.
   * Each call to this method will build a different instance of a classloader.
   *
   * Samza framework API classes need to be specified in a file called
   * {@link DependencyIsolationUtils#FRAMEWORK_API_CLASS_LIST_FILE_NAME} which is in the lib directory which is in the
   * API package. The file needs to be generated when building the framework API package. This class will not generate
   * the file.
   *
   * Implementation notes:
   *
   * The cytodynamics isolating classloader is used for this. It provides more control than the built-in
   * {@link URLClassLoader}. Cytodynamics provides the ability to compose multiple classloaders together and have more
   * granular delegation strategies between the classloaders.
   *
   * In order to share objects between classes loaded by different classloaders, the classes for the shared objects must
   * be loaded by a common classloader. Those common classes will be loaded through a common API classloader. The
   * cytodynamics classloader can be set up to only use the common API classloader for an explicit set of classes. The
   * {@link DependencyIsolationUtils#FRAMEWORK_API_CLASS_LIST_FILE_NAME} file should include the framework API classes.
   * Also, bootstrap classes (e.g. java.lang.String) need to be loaded by a common classloader, since objects of those
   * types need to be shared across different framework and application. There are also some static bootstrap classes
   * which should be shared (e.g. java.lang.System). Bootstrap classes will be loaded through a common classloader by
   * default.
   *
   * These are the classloaders which are used to make up the final classloader.
   * <ul>
   *   <li>bootstrap classloader: Built-in Java classes (e.g. java.lang.String)</li>
   *   <li>API classloader: Common Samza framework API classes</li>
   *   <li>infrastructure classloader: Core Samza framework classes and plugins that are included in the framework</li>
   *   <li>
   *     application classloader: Application code and plugins that are needed in the app but are not included in the
   *     framework
   *   </li>
   * </ul>
   *
   * This is the delegation structure for the classloaders:
   * <pre>
   *   (bootstrap               (API                  (application
   *   classloader) &lt;---- classloader) &lt;------- classloader)
   *                             ^                      ^
   *                             |                     /
   *                             |                    /
   *                             |                   /
   *                             |                  /
   *                         (infrastructure classloader)
   * </pre>
   * The cytodynamics classloader allows control over when the delegation should happen.
   * <ol>
   *   <li>API classloader delegates to the bootstrap classloader if the bootstrap classloader has the class.</li>
   *   <li>
   *     Infrastructure classloader only delegates to the API classloader for the common classes specified by
   *     {@link DependencyIsolationUtils#FRAMEWORK_API_CLASS_LIST_FILE_NAME}.
   *   </li>
   *   <li>
   *     Infrastructure classloader delegates to the application classloader when a class can't be found in the
   *     infrastructure classloader.
   *   </li>
   *   <li>
   *     Application classloader only delegates to the API classloader for the common classes specified by
   *     {@link DependencyIsolationUtils#FRAMEWORK_API_CLASS_LIST_FILE_NAME}.
   *   </li>
   * </ol>
   */
  public ClassLoader buildMainClassLoader() {
    File apiLibDirectory = buildApiLibDirectory();
    LOG.info("Using API lib directory: {}", apiLibDirectory);
    File infrastructureLibDirectory = buildInfrastructureLibDirectory();
    LOG.info("Using infrastructure lib directory: {}", infrastructureLibDirectory);
    File applicationLibDirectory = buildApplicationLibDirectory();
    LOG.info("Using application lib directory: {}", applicationLibDirectory);

    ClassLoader apiClassLoader = buildApiClassLoader(apiLibDirectory);
    ClassLoader applicationClassLoader =
        buildApplicationClassLoader(applicationLibDirectory, apiLibDirectory, apiClassLoader);

    // the classloader to return is the one with the infrastructure classpath
    return buildInfrastructureClassLoader(infrastructureLibDirectory, apiLibDirectory, apiClassLoader,
        applicationClassLoader);
  }

  /**
   * Build a classloader for loading {@link org.apache.samza.application.SamzaApplication} when in isolation mode.
   *
   * It is not sufficient to use the classloader from {@link #buildMainClassLoader()} to load the
   * {@link org.apache.samza.application.SamzaApplication}, because the describe method might need to use classes in the
   * framework infrastructure classloader (e.g. framework descriptors). The classloader from
   * {@link #buildMainClassLoader()} would not be able to delegate from the application classloader to the framework
   * infrastructure classloader.
   *
   * If the classloader from {@link #buildMainClassLoader()} is passed as the {@code parentClassLoader}, then the
   * describe method can use framework infrastructure components and application-specific components.
   *
   * Implementation notes:
   *
   * This classloader will only directly load the {@link org.apache.samza.application.SamzaApplication} class, and it
   * will delegate all other classloading to the {@code parentClassLoader}. This allows framework descriptors to be
   * loaded from the framework infrastructure classloader. In addition, this allows the application processing classes
   * to be loaded from the initial application classloader when calling describe. This is desirable because the core
   * processing flows will delegate to the initial application classloader as well, and it is necessary that the same
   * application classloader is used for all classes in the application processing flows.
   *
   * @param samzaApplicationClassName Class name of the {@link org.apache.samza.application.SamzaApplication}
   * @param parentClassLoader {@link ClassLoader} to delegate to. Should be the {@link #buildMainClassLoader()}.
   * @return {@link ClassLoader} to use for loading the {@link org.apache.samza.application.SamzaApplication}
   */
  public ClassLoader buildSamzaApplicationClassLoader(String samzaApplicationClassName,
      ClassLoader parentClassLoader) {
    File applicationLibDirectory = buildApplicationLibDirectory();
    return LoaderBuilder.anIsolatingLoader()
        // SamzaApplication is in the application classpath
        .withClasspath(getClasspathAsURIs(applicationLibDirectory))
        // getClasspathAsURIs should only return JARs within applicationLibDirectory anyways, but doing it to be safe
        .withOriginRestriction(OriginRestriction.denyByDefault().allowingDirectory(applicationLibDirectory, false))
        .withParentRelationship(DelegateRelationshipBuilder.builder()
            .withDelegateClassLoader(parentClassLoader)
            // almost everything should come from the parent classloader
            .addDelegatePreferredClassPredicate(new GlobMatcher("*"))
            // blacklisting the SamzaApplication class from the parent so that it gets loaded by this classloader
            .addBlacklistedClassPredicate(new GlobMatcher(samzaApplicationClassName))
            // combined with blacklisting, FULL ensures that the SamzaApplication directly comes from this classloader
            .withIsolationLevel(IsolationLevel.FULL)
            .build())
        .build();
  }

  /**
   * Build the {@link ClassLoader} which can load framework API classes.
   *
   * This sets up the link between the bootstrap classloader and the API classloader (see {@link #buildMainClassLoader()}.
   */
  private static ClassLoader buildApiClassLoader(File apiLibDirectory) {
    /*
     * This can just use the built-in classloading, which checks the parent classloader first and then checks its own
     * classpath. A null parent means bootstrap classloader, which contains core Java classes (e.g. java.lang.String).
     * This doesn't need to be isolated from the parent, because we only want to load all bootstrap classes from the
     * bootstrap classloader.
     */
    return new URLClassLoader(getClasspathAsURLs(apiLibDirectory), null);
  }

  /**
   * Build the {@link ClassLoader} which can load application classes.
   *
   * This sets up the link between the application classloader and the API classloader (see {@link #buildMainClassLoader()}.
   */
  private static ClassLoader buildApplicationClassLoader(File applicationLibDirectory, File apiLibDirectory,
      ClassLoader apiClassLoader) {
    return LoaderBuilder.anIsolatingLoader()
        // look in application lib directory for JARs
        .withClasspath(getClasspathAsURIs(applicationLibDirectory))
        // getClasspathAsURIs should only return JARs within applicationLibDirectory anyways, but doing it to be safe
        .withOriginRestriction(OriginRestriction.denyByDefault().allowingDirectory(applicationLibDirectory, false))
        // delegate to the api classloader for API classes
        .withParentRelationship(buildApiParentRelationship(apiLibDirectory, apiClassLoader))
        .build();
  }

  /**
   * Build the {@link ClassLoader} which can load Samza framework core classes. If a file with the name
   * {@link DependencyIsolationUtils#RUNTIME_FRAMEWORK_RESOURCES_PATHING_JAR_NAME} is found in {@code baseJobDirectory},
   * then it will be included in the classpath.
   * This may also fall back to loading application classes.
   *
   * This sets up two links: One link between the infrastructure classloader and the API and another link between the
   * infrastructure classloader and the application classloader (see {@link #buildMainClassLoader()}.
   */
  private static ClassLoader buildInfrastructureClassLoader(File infrastructureLibDirectory,
      File apiLibDirectory,
      ClassLoader apiClassLoader,
      ClassLoader applicationClassLoader) {
    // start with JARs in infrastructure lib directory
    List<URI> classpathURIs = new ArrayList<>(getClasspathAsURIs(infrastructureLibDirectory));
    OriginRestriction originRestriction = OriginRestriction.denyByDefault()
        // getClasspathAsURIs should only return JARs within infrastructureLibDirectory anyways, but doing it to be safe
        .allowingDirectory(infrastructureLibDirectory, false);
    File runtimeFrameworkResourcesPathingJar =
        new File(getBaseJobDirectory(), DependencyIsolationUtils.RUNTIME_FRAMEWORK_RESOURCES_PATHING_JAR_NAME);
    if (canAccess(runtimeFrameworkResourcesPathingJar)) {
      // if there is a runtime framework resources pathing JAR, then include that in the classpath as well
      classpathURIs.add(runtimeFrameworkResourcesPathingJar.toURI());
      originRestriction.allowingGlobPattern(fileURL(runtimeFrameworkResourcesPathingJar).toExternalForm());
      LOG.info("Added {} to infrastructure classpath", runtimeFrameworkResourcesPathingJar.getPath());
    } else {
      LOG.info("Unable to access {}, so not adding to infrastructure classpath",
          runtimeFrameworkResourcesPathingJar.getPath());
    }
    return LoaderBuilder.anIsolatingLoader()
        .withClasspath(Collections.unmodifiableList(classpathURIs))
        .withOriginRestriction(originRestriction)
        .withParentRelationship(buildApiParentRelationship(apiLibDirectory, apiClassLoader))
        /*
         * Fall back to the application classloader for certain classes. For example, the application might implement
         * some pluggable classes (e.g. SystemFactory). Another example is message schemas that are supplied by the
         * application.
         */
        .addFallbackDelegate(DelegateRelationshipBuilder.builder()
            .withDelegateClassLoader(applicationClassLoader)
            /*
             * NONE means that a class will be loaded from here if it is not found in the classpath of the loader that uses
             * this relationship.
             */
            .withIsolationLevel(IsolationLevel.NONE)
            .build())
        .build();
  }

  /**
   * Build a {@link DelegateRelationship} which defines how to delegate to the API classloader.
   *
   * Delegation will only happen for classes specified in
   * {@link DependencyIsolationUtils#FRAMEWORK_API_CLASS_LIST_FILE_NAME} and the Java bootstrap classes.
   */
  private static DelegateRelationship buildApiParentRelationship(File apiLibDirectory, ClassLoader apiClassLoader) {
    DelegateRelationshipBuilder apiParentRelationshipBuilder = DelegateRelationshipBuilder.builder()
        // needs to load API classes from the API classloader
        .withDelegateClassLoader(apiClassLoader)
        /*
         * FULL means to only load classes explicitly specified as "API" from the API classloader. We will use
         * delegate-preferred class predicates to specify which classes are "API" (see below).
         */
        .withIsolationLevel(IsolationLevel.FULL);

    // bootstrap classes need to be loaded from a common classloader
    apiParentRelationshipBuilder.addDelegatePreferredClassPredicate(new BootstrapClassPredicate());
    // the classes which are Samza framework API classes are added here
    getFrameworkApiClassGlobs(apiLibDirectory).forEach(
        apiClassName -> apiParentRelationshipBuilder.addDelegatePreferredClassPredicate(new GlobMatcher(apiClassName)));
    return apiParentRelationshipBuilder.build();
  }

  /**
   * Gets the globs for matching against classes to load from the framework API classloader. This will read the
   * {@link DependencyIsolationUtils#FRAMEWORK_API_CLASS_LIST_FILE_NAME} file in {@code directoryWithClassList} to get
   * the globs.
   *
   * @param directoryWithClassList Directory in which
   * {@link DependencyIsolationUtils#FRAMEWORK_API_CLASS_LIST_FILE_NAME} lives
   * @return {@link List} of globs for matching against classes to load from the framework API classloader
   */
  @VisibleForTesting
  static List<String> getFrameworkApiClassGlobs(File directoryWithClassList) {
    File parentPreferredFile =
        new File(directoryWithClassList, DependencyIsolationUtils.FRAMEWORK_API_CLASS_LIST_FILE_NAME);
    validateCanAccess(parentPreferredFile);
    try {
      return Files.readAllLines(Paths.get(parentPreferredFile.toURI()), StandardCharsets.UTF_8)
          .stream()
          .filter(StringUtils::isNotBlank)
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new SamzaException("Error while reading samza-api class list", e);
    }
  }

  /**
   * Get the {@link URL}s of all JARs/WARs in the directory {@code jarsLocation}. This only looks one level down; it is
   * not recursive.
   */
  @VisibleForTesting
  static URL[] getClasspathAsURLs(File jarsLocation) {
    validateCanAccess(jarsLocation);
    File[] filesInJarsLocation = jarsLocation.listFiles();
    if (filesInJarsLocation == null) {
      throw new SamzaException(
          String.format("Could not find any files inside %s, probably because it is not a directory",
              jarsLocation.getPath()));
    }
    URL[] urls = Stream.of(filesInJarsLocation)
        .filter(file -> file.getName().endsWith(".jar") || file.getName().endsWith(".war"))
        .map(IsolatingClassLoaderFactory::fileURL)
        .toArray(URL[]::new);
    LOG.info("Found {} items to load into classpath from {}", urls.length, jarsLocation);
    Stream.of(urls).forEach(url -> LOG.debug("Found {} from {}", url, jarsLocation));
    return urls;
  }

  /**
   * Get the {@link URI}s of all JARs/WARs in the directory {@code jarsLocation}. This only looks one level down; it is
   * not recursive.
   */
  @VisibleForTesting
  static List<URI> getClasspathAsURIs(File jarsLocation) {
    return Stream.of(getClasspathAsURLs(jarsLocation))
        .map(IsolatingClassLoaderFactory::urlToURI)
        .collect(Collectors.toList());
  }

  private static boolean canAccess(File file) {
    return file.exists() && file.canRead();
  }

  /**
   * Makes sure that a file exists and can be read.
   */
  private static void validateCanAccess(File file) {
    if (!canAccess(file)) {
      throw new SamzaException("Unable to access file: " + file);
    }
  }

  /**
   * Get the {@link URL} for a {@link File}.
   * Converts checked exceptions into {@link SamzaException}s.
   */
  private static URL fileURL(File file) {
    URI uri = file.toURI();
    try {
      return uri.toURL();
    } catch (MalformedURLException e) {
      throw new SamzaException("Unable to get URL for file: " + file, e);
    }
  }

  /**
   * Get the {@link URI} for a {@link URL}.
   * Converts checked exceptions into {@link SamzaException}s.
   */
  private static URI urlToURI(URL url) {
    try {
      return url.toURI();
    } catch (URISyntaxException e) {
      throw new SamzaException("Unable to get URI for URL: " + url, e);
    }
  }

  /**
   * Build the {@link File} which is a directory in which to look for framework API resources.
   * @return {@link File} which is a directory in which to look for framework API resources
   */
  private static File buildApiLibDirectory() {
    return libDirectory(new File(getBaseJobDirectory(), DependencyIsolationUtils.FRAMEWORK_API_DIRECTORY));
  }

  /**
   * Build the {@link File} which is a directory in which to look for framework infrastructure resources.
   * @return {@link File} which is a directory in which to look for framework infrastructure resources
   */
  private static File buildInfrastructureLibDirectory() {
    return libDirectory(new File(getBaseJobDirectory(), DependencyIsolationUtils.FRAMEWORK_INFRASTRUCTURE_DIRECTORY));
  }

  /**
   * Build the {@link File} which is a directory in which to look for application resources.
   * @return {@link File} which is a directory in which to look for application resources
   */
  private static File buildApplicationLibDirectory() {
    return libDirectory(new File(getBaseJobDirectory(), DependencyIsolationUtils.APPLICATION_DIRECTORY));
  }

  /**
   * Build the {@link File} which is the parent directory to all of the directories that contain the resources to run
   * the Samza application (directories for both framework resources and application resources).
   * @return {@link File} which is the parent directory to all directories with resources for running the app
   */
  private static File getBaseJobDirectory() {
    // start at the user.dir to find the resources for the classpaths
    return new File(System.getProperty("user.dir"));
  }

  /**
   * Get the {@link File} representing the {@link #LIB_DIRECTORY} inside the given {@code file}.
   */
  private static File libDirectory(File file) {
    return new File(file, LIB_DIRECTORY);
  }
}
