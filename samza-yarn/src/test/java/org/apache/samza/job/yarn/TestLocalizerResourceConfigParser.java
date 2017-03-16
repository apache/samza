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
package org.apache.samza.job.yarn;

import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestLocalizerResourceConfigParser {

  @Rule
  public ExpectedException thrown= ExpectedException.none();

  @Test
  public void testParserSuccessWithPrivateArchive() throws Exception {
    String key = "yarn.localizer.resource.private.archive.__package";
    String val = "http://host.com/service?pkg=abc";
    LocalizerResourceConfigParser parser = new LocalizerResourceConfigParser(key, val);
    assertTrue("all fields are valid for parser", parser.isValid());
    assertEquals("visibility is PRIVATE", "PRIVATE", parser.getResourceVisibility().toString());
    assertEquals("type is ARCHIVE", "ARCHIVE", parser.getResourceType().toString());
    assertEquals("name is __package", "__package", parser.getResourceName());
    assertEquals("path is "+val, val, parser.getResourcePath().toString());
  }

  @Test
  public void testParserSuccessWithApplicationFile() throws Exception {
    String key = "yarn.localizer.resource.application.file.identity.p12";
    String val = "certfs://host.com/service?param=abc";
    LocalizerResourceConfigParser parser = new LocalizerResourceConfigParser(key, val);
    assertTrue("all fields are valid for parser", parser.isValid());
    assertEquals("visibility is APPLICATION", "APPLICATION", parser.getResourceVisibility().toString());
    assertEquals("type is FILE", "FILE", parser.getResourceType().toString());
    assertEquals("name is identity.p12", "identity.p12", parser.getResourceName());
    assertEquals("path is "+val, val, parser.getResourcePath().toString());
  }

  @Test
  public void testInvalidConfigKey() throws Exception {
    String key = "invalid.localizer.resource.private.archive.__package";
    String val = "http://host.com/service?pkg=abc";
    thrown.expect(LocalizerResourceException.class);
    thrown.expectMessage("Invalid localizer resource config key");
    LocalizerResourceConfigParser parser = new LocalizerResourceConfigParser(key, val);
  }

  @Test
  public void testInvalidVisibility() throws Exception {
    String key = "yarn.localizer.resource.invalidVis.archive.__package";
    String val = "http://host.com/service?pkg=abc";
    thrown.expect(LocalizerResourceException.class);
    thrown.expectMessage("Invalid resource visibility or type from localizer resource config key");
    LocalizerResourceConfigParser parser = new LocalizerResourceConfigParser(key, val);
  }

  @Test
  public void testInvalidType() throws Exception {
    String key = "yarn.localizer.resource.application.invalidType.__package";
    String val = "http://host.com/service?pkg=abc";
    thrown.expect(LocalizerResourceException.class);
    thrown.expectMessage("Invalid resource visibility or type from localizer resource config key");
    LocalizerResourceConfigParser parser = new LocalizerResourceConfigParser(key, val);
  }

  @Test
  public void testInvalidPath() throws Exception {
    String key = "yarn.localizer.resource.application.archive.__package";
    String val = "";
    thrown.expect(LocalizerResourceException.class);
    thrown.expectMessage("Invalid localizer resource config value");
    LocalizerResourceConfigParser parser = new LocalizerResourceConfigParser(key, val);
  }

  @Test
  public void testIsValid() throws Exception {
    String key = "yarn.localizer.resource.application.archive.";
    String val = "http://host.com/service?pkg=abc";
    LocalizerResourceConfigParser parser = new LocalizerResourceConfigParser(key, val);
    assertFalse("isValid is false", parser.isValid());
  }
}
