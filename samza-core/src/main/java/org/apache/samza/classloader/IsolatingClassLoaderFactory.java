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

import com.google.common.collect.ImmutableSet;
import com.linkedin.cytodynamics.matcher.BootstrapClassPredicate;
import com.linkedin.cytodynamics.matcher.GlobMatcher;
import com.linkedin.cytodynamics.nucleus.DelegateRelationship;
import com.linkedin.cytodynamics.nucleus.DelegateRelationshipBuilder;
import com.linkedin.cytodynamics.nucleus.IsolationLevel;
import com.linkedin.cytodynamics.nucleus.LoaderBuilder;
import com.linkedin.cytodynamics.nucleus.OriginRestriction;
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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.google.common.annotations.VisibleForTesting;
import org.apache.samza.SamzaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Use this to build an isolating classloader for running Samza.
 */
public class IsolatingClassLoaderFactory {
  private static final Logger LOG = LoggerFactory.getLogger(IsolatingClassLoaderFactory.class);
  private static final String SAMZA_FRAMEWORK_API_CLASS_LIST_FILE_NAME = "samza-framework-api-class-list.txt";

  public ClassLoader buildClassLoader() {
    String baseDirectoryPath = System.getProperty("user.dir");
    File apiLibDirectory =
        libDirectory(new File(baseDirectoryPath, IsolationUtils.APPLICATION_MASTER_API_DIRECTORY));
    LOG.info("Using API lib directory: {}", apiLibDirectory);
    File infrastructureLibDirectory =
        libDirectory(new File(baseDirectoryPath, IsolationUtils.APPLICATION_MASTER_INFRASTRUCTURE_DIRECTORY));
    LOG.info("Using infrastructure lib directory: {}", infrastructureLibDirectory);
    File applicationLibDirectory =
        libDirectory(new File(baseDirectoryPath, IsolationUtils.APPLICATION_MASTER_APPLICATION_DIRECTORY));
    LOG.info("Using application lib directory: {}", applicationLibDirectory);

    ClassLoader apiClassLoader = buildApiClassLoader(apiLibDirectory);
    ClassLoader applicationClassLoader =
        buildApplicationClassLoader(applicationLibDirectory, apiLibDirectory, apiClassLoader);

    // the classloader to ultimately return is the one with the infrastructure classpath
    return buildInfrastructureClassLoader(infrastructureLibDirectory, apiLibDirectory, apiClassLoader,
        applicationClassLoader);
  }

  private static ClassLoader buildApiClassLoader(File apiLibDirectory) {
    // null parent means to use bootstrap classloader as the parent
    return new URLClassLoader(getClasspathAsURLs(apiLibDirectory), null);
  }

  private static ClassLoader buildApplicationClassLoader(File applicationLibDirectory, File apiLibDirectory,
      ClassLoader apiClassLoader) {
    return LoaderBuilder.anIsolatingLoader()
        // look in application directory for JARs
        .withClasspath(getClasspathAsURIs(applicationLibDirectory))
        // getClasspathAsURIs should already satisfy this, but doing it to be safe
        .withOriginRestriction(OriginRestriction.denyByDefault().allowingDirectory(applicationLibDirectory, false))
        .withParentRelationship(buildApiParentRelationship(apiLibDirectory, apiClassLoader))
        .build();
  }

  private static ClassLoader buildInfrastructureClassLoader(File infrastructureLibDirectory, File apiLibDirectory,
      ClassLoader apiClassLoader, ClassLoader applicationClassLoader) {
    return LoaderBuilder.anIsolatingLoader()
        // look in application directory for JARs
        .withClasspath(getClasspathAsURIs(infrastructureLibDirectory))
        // getClasspathAsURIs should already satisfy this, but doing it to be safe
        .withOriginRestriction(OriginRestriction.denyByDefault().allowingDirectory(infrastructureLibDirectory, false))
        .withParentRelationship(buildApiParentRelationship(apiLibDirectory, apiClassLoader))
        // need to be able to fall back to application for loading certain classes
        .addFallbackDelegate(buildFallbackDelegateRelationship(applicationClassLoader))
        .build();
  }

  private static DelegateRelationship buildApiParentRelationship(File apiLibDirectory, ClassLoader apiClassLoader) {
    DelegateRelationshipBuilder apiParentRelationshipBuilder = DelegateRelationshipBuilder.builder()
        // needs to load API classes from the API classloader
        .withDelegateClassLoader(apiClassLoader)
        // use FULL to only load API classes from API classloader
        .withIsolationLevel(IsolationLevel.FULL);
    /*
     * All bootstrap classes (e.g. java.lang classes) should be accessible and loaded from a single classloader, so that
     * they are the same across the whole JVM. The API classloader has the bootstrap classes.
     */
    apiParentRelationshipBuilder.addDelegatePreferredClassPredicate(new BootstrapClassPredicate());
    // add the classes which are API classes
    getApiClasses(apiLibDirectory).forEach(
        apiClassName -> apiParentRelationshipBuilder.addDelegatePreferredClassPredicate(new GlobMatcher(apiClassName)));
    return apiParentRelationshipBuilder.build();
  }

  private static DelegateRelationship buildFallbackDelegateRelationship(ClassLoader fallbackClassLoader) {
    return DelegateRelationshipBuilder.builder()
        .withDelegateClassLoader(fallbackClassLoader)
        // want to load any class from the fallback
        .withIsolationLevel(IsolationLevel.NONE)
        .build();
  }

  @VisibleForTesting
  static List<String> getApiClasses(File directoryWithClassList) {
    File parentPreferredFile = new File(directoryWithClassList, SAMZA_FRAMEWORK_API_CLASS_LIST_FILE_NAME);
    validateCanAccess(parentPreferredFile);
    try {
      return Files.readAllLines(Paths.get(parentPreferredFile.toURI()), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new SamzaException("Error while reading samza-api class list", e);
    }
  }

  /**
   * @param jarsLocation directory where JARs are located
   * @return URLs for all JARs/WARs in the jarsLocation
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
    return urls;
  }

  @VisibleForTesting
  static List<URI> getClasspathAsURIs(File jarsLocation) {
    return Stream.of(getClasspathAsURLs(jarsLocation))
        .map(IsolatingClassLoaderFactory::urlToURI)
        .collect(Collectors.toList());
  }

  private static void validateCanAccess(File file) {
    if (!file.exists() || !file.canRead()) {
      throw new SamzaException("Unable to access file: " + file);
    }
  }

  private static URL fileURL(File file) {
    URI uri = file.toURI();
    try {
      return uri.toURL();
    } catch (MalformedURLException e) {
      throw new SamzaException("Unable to get URL for file: " + file, e);
    }
  }

  private static URI urlToURI(URL url) {
    try {
      return url.toURI();
    } catch (URISyntaxException e) {
      throw new SamzaException("Unable to get URI for URL: " + url, e);
    }
  }

  private static File libDirectory(File file) {
    return new File(file, "lib");
  }
}
