package org.apache.samza.classloader;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.samza.SamzaException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestIsolatingClassLoaderFactory {
  @Test
  public void testGetApiClasses() throws URISyntaxException {
    File apiClassListFile = Paths.get(getClass().getResource("/classloader").toURI()).toFile();
    List<String> apiClassNames = IsolatingClassLoaderFactory.getApiClasses(apiClassListFile);
    List<String> expected = ImmutableList.of(
        "org.apache.samza.JavaClass",
        "org.apache.samza.JavaClass$InnerJavaClass",
        "org.apache.samza.ScalaClass$",
        "org.apache.samza.ScalaClass$$anon$1",
        "my.package.with.wildcard.*",
        "my.package.with.question.mark?");
    assertEquals(expected, apiClassNames);
  }

  @Test(expected = SamzaException.class)
  public void testGetApiClassesFileDoesNotExist() throws URISyntaxException {
    File nonExistentDirectory =
        new File(Paths.get(getClass().getResource("/classloader").toURI()).toFile(), "doesNotExist");
    IsolatingClassLoaderFactory.getApiClasses(nonExistentDirectory);
  }

  @Test
  public void testGetClasspathAsURLs() throws URISyntaxException {
    File classpathDirectory = Paths.get(getClass().getResource("/classloader/classpath").toURI()).toFile();
    URL[] classpath = IsolatingClassLoaderFactory.getClasspathAsURLs(classpathDirectory);
    assertEquals(2, classpath.length);
    Set<URL> classpathSet = ImmutableSet.copyOf(classpath);
    URL jarUrl = getClass().getResource("/classloader/classpath/placeholder-jar.jar");
    assertTrue(classpathSet.contains(jarUrl));
    URL warUrl = getClass().getResource("/classloader/classpath/placeholder-war.war");
    assertTrue(classpathSet.contains(warUrl));
  }

  @Test(expected = SamzaException.class)
  public void testGetClasspathAsURLsDirectoryDoesNotExist() throws URISyntaxException {
    File nonExistentDirectory =
        new File(Paths.get(getClass().getResource("/classloader").toURI()).toFile(), "doesNotExist");
    IsolatingClassLoaderFactory.getClasspathAsURLs(nonExistentDirectory);
  }

  @Test
  public void testGetClasspathAsURIs() throws URISyntaxException {
    File classpathDirectory = Paths.get(getClass().getResource("/classloader/classpath").toURI()).toFile();
    List<URI> classpath = IsolatingClassLoaderFactory.getClasspathAsURIs(classpathDirectory);
    assertEquals(2, classpath.size());
    Set<URI> classpathSet = ImmutableSet.copyOf(classpath);
    URL jarUrl = getClass().getResource("/classloader/classpath/placeholder-jar.jar");
    assertTrue(classpathSet.contains(jarUrl.toURI()));
    URL warUrl = getClass().getResource("/classloader/classpath/placeholder-war.war");
    assertTrue(classpathSet.contains(warUrl.toURI()));
  }

  @Test(expected = SamzaException.class)
  public void testGetClasspathAsURIsDirectoryDoesNotExist() throws URISyntaxException {
    File nonExistentDirectory =
        new File(Paths.get(getClass().getResource("/classloader").toURI()).toFile(), "doesNotExist");
    IsolatingClassLoaderFactory.getClasspathAsURIs(nonExistentDirectory);
  }
}