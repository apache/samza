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
package org.apache.samza.rest.resources;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.Response;
import org.apache.samza.config.MapConfig;
import org.apache.samza.rest.SamzaRestApplication;
import org.apache.samza.rest.SamzaRestConfig;
import org.apache.samza.rest.model.Job;
import org.apache.samza.rest.model.JobStatus;
import org.apache.samza.rest.resources.mock.MockJobProxy;
import org.apache.samza.rest.resources.mock.MockJobProxyFactory;
import org.apache.samza.rest.resources.mock.MockResourceFactory;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class TestJobsResource extends JerseyTest {
  ObjectMapper objectMapper = SamzaObjectMapper.getObjectMapper();

  @Override
  protected Application configure() {
    Map<String, String> configMap = ImmutableMap.of(JobsResourceConfig.CONFIG_JOB_PROXY_FACTORY,
                                                    MockJobProxyFactory.class.getName(),
                                                    SamzaRestConfig.CONFIG_REST_RESOURCE_FACTORIES,
                                                    MockResourceFactory.class.getName());
    SamzaRestConfig config = new SamzaRestConfig(new MapConfig(configMap));
    return new SamzaRestApplication(config);
  }

  @Test
   public void testGetJobs()
      throws IOException {

    Response resp = target("v1/jobs").request().get();
    assertEquals(200, resp.getStatus());
    final Job[] jobs = objectMapper.readValue(resp.readEntity(String.class), Job[].class);
    assertEquals(4, jobs.length);

    assertEquals(MockJobProxy.JOB_INSTANCE_1_NAME, jobs[0].getJobName());
    assertEquals(MockJobProxy.JOB_INSTANCE_1_ID, jobs[0].getJobId());
    assertStatusNotDefault(jobs[0]);
    assertEquals(MockJobProxy.JOB_INSTANCE_2_NAME, jobs[1].getJobName());
    assertEquals(MockJobProxy.JOB_INSTANCE_2_ID, jobs[1].getJobId());
    assertStatusNotDefault(jobs[1]);
    assertEquals(MockJobProxy.JOB_INSTANCE_3_NAME, jobs[2].getJobName());
    assertEquals(MockJobProxy.JOB_INSTANCE_3_ID, jobs[2].getJobId());
    assertStatusNotDefault(jobs[2]);
    assertEquals(MockJobProxy.JOB_INSTANCE_4_NAME, jobs[3].getJobName());
    assertEquals(MockJobProxy.JOB_INSTANCE_4_ID, jobs[3].getJobId());
    assertStatusNotDefault(jobs[3]);
    resp.close();
  }

  @Test
   public void testPostJobs()
      throws IOException {
    Response resp = target("v1/jobs").request().post(Entity.text(""));
    assertEquals(405, resp.getStatus());
    resp.close();
  }

  @Test
  public void testPutJobs()
      throws IOException {
    Response resp = target("v1/jobs").request().put(Entity.text(""));
    assertEquals(405, resp.getStatus());
    resp.close();
  }

  @Test
  public void testGetJob()
      throws IOException {
    Response resp = target(String.format("v1/jobs/%s/%s", MockJobProxy.JOB_INSTANCE_2_NAME, MockJobProxy.JOB_INSTANCE_2_ID)).request().get();
    assertEquals(200, resp.getStatus());
    final Job job2 = objectMapper.readValue(resp.readEntity(String.class), Job.class);

    assertEquals(MockJobProxy.JOB_INSTANCE_2_NAME, job2.getJobName());
    assertEquals(MockJobProxy.JOB_INSTANCE_2_ID, job2.getJobId());
    assertStatusNotDefault(job2);
    resp.close();
  }

  @Test
  public void testPostJob()
      throws IOException {
    Response resp = target(String.format("v1/jobs/%s/%s", MockJobProxy.JOB_INSTANCE_2_NAME, MockJobProxy.JOB_INSTANCE_2_ID)).request().post(
        Entity.text(""));
    assertEquals(405, resp.getStatus());
    resp.close();
  }

  @Test
  public void testGetJobNameNotFound()
      throws IOException {
    Response resp = target(String.format("v1/jobs/%s/%s", "BadJobName", MockJobProxy.JOB_INSTANCE_2_ID)).request().get();
    assertEquals(404, resp.getStatus());

    final Map<String, String> errorMessage = objectMapper.readValue(resp.readEntity(String.class), new TypeReference<Map<String, String>>() {});
    assertTrue(errorMessage.get("message"), errorMessage.get("message").contains("does not exist"));
    resp.close();
  }

  @Test
  public void testGetJobIdNotFound()
      throws IOException {
    Response resp = target(String.format("v1/jobs/%s/%s", MockJobProxy.JOB_INSTANCE_2_NAME, "BadJobId")).request().get();
    assertEquals(404, resp.getStatus());

    final Map<String, String> errorMessage = objectMapper.readValue(resp.readEntity(String.class), new TypeReference<Map<String, String>>() {});
    assertTrue(errorMessage.get("message"), errorMessage.get("message").contains("does not exist"));
    resp.close();
  }

  @Test
  public void testGetJobNameWithoutId()
      throws IOException {
    Response resp = target(String.format("v1/jobs/%s", MockJobProxy.JOB_INSTANCE_2_NAME)).request().get();
    assertEquals(404, resp.getStatus());
    resp.close();
  }

  @Test
  public void testStartJob()
      throws IOException {
    Response resp = target(String.format("v1/jobs/%s/%s", MockJobProxy.JOB_INSTANCE_2_NAME, MockJobProxy.JOB_INSTANCE_2_ID))
        .queryParam("status", "started").request().put(Entity.form(new Form()));
    assertEquals(202, resp.getStatus());

    final Job job2 = objectMapper.readValue(resp.readEntity(String.class), Job.class);
    assertEquals(MockJobProxy.JOB_INSTANCE_2_NAME, job2.getJobName());
    assertEquals(MockJobProxy.JOB_INSTANCE_2_ID, job2.getJobId());
    assertStatusNotDefault(job2);
    resp.close();
  }

  @Test
  public void testStopJob()
      throws IOException {
    Response resp = target(String.format("v1/jobs/%s/%s", MockJobProxy.JOB_INSTANCE_2_NAME, MockJobProxy.JOB_INSTANCE_2_ID))
        .queryParam("status", "stopped").request().put(Entity.form(new Form()));
    assertEquals(202, resp.getStatus());

    final Job job2 = objectMapper.readValue(resp.readEntity(String.class), Job.class);
    assertEquals(MockJobProxy.JOB_INSTANCE_2_NAME, job2.getJobName());
    assertEquals(MockJobProxy.JOB_INSTANCE_2_ID, job2.getJobId());
    assertStatusNotDefault(job2);
    resp.close();
  }

  @Test
  public void testPutBadJobStatus()
      throws IOException {
    Response resp = target(String.format("v1/jobs/%s/%s", MockJobProxy.JOB_INSTANCE_2_NAME, MockJobProxy.JOB_INSTANCE_2_ID))
        .queryParam("status", "BADSTATUS").request().put(Entity.form(new Form()));
    assertEquals(400, resp.getStatus());

    final Map<String, String> errorMessage = objectMapper.readValue(resp.readEntity(String.class), new TypeReference<Map<String, String>>() {});
    assertTrue(errorMessage.get("message").contains("BADSTATUS"));
    resp.close();
  }

  @Test
  public void testPutMissingStatus()
      throws IOException {
    Response resp = target(String.format("v1/jobs/%s/%s", MockJobProxy.JOB_INSTANCE_2_NAME, MockJobProxy.JOB_INSTANCE_2_ID)).request()
        .put(Entity.form(new Form()));
    assertEquals(400, resp.getStatus());

    final Map<String, String> errorMessage = objectMapper.readValue(resp.readEntity(String.class), new TypeReference<Map<String, String>>() {});
    assertTrue(errorMessage.get("message").contains("status"));
    resp.close();
  }

  private void assertStatusNotDefault(Job job)  {
    // Job status should be populated, not the defaults.
    // We're not testing whether it value matches a specific value here,
    // just that the value reflects whatever the JobStatusProvider returns.
    assertFalse(JobStatus.UNKNOWN == job.getStatus());
    assertNotNull(job.getStatusDetail());
  }
}
