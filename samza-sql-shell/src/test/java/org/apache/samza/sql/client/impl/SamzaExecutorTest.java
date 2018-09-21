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

package org.apache.samza.sql.client.impl;

import java.util.List;
import org.apache.samza.sql.client.interfaces.QueryResult;
import org.apache.samza.sql.client.interfaces.SqlSchema;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;


public class SamzaExecutorTest {
    private SamzaExecutor m_executor = new SamzaExecutor();

    /**
     * Need a local Kafka cluster
     */
    @Ignore
    @Test
    public void testQueryResult() {
        String sql = "select * from kafka.ProfileChangeStream";
        QueryResult queryResult = m_executor.executeQuery(null, sql);
        SqlSchema ts = queryResult.getSchema();

        Assert.assertEquals("__key__", ts.getFieldName(0));
        Assert.assertEquals("Name", ts.getFieldName(1));
        Assert.assertEquals("NewCompany", ts.getFieldName(2));
        Assert.assertEquals("OldCompany", ts.getFieldName(3));
        Assert.assertEquals("ProfileChangeTimestamp", ts.getFieldName(4));

        /*Assert.assertEquals("VARCHAR", ts.getFieldTypeName(0));
        Assert.assertEquals("VARCHAR", ts.getFieldTypeName(1));
        Assert.assertEquals("VARCHAR", ts.getFieldTypeName(2));
        Assert.assertEquals("VARCHAR", ts.getFieldTypeName(3));
        Assert.assertEquals("BIGINT", ts.getFieldTypeName(4));*/

        try {
          Thread.sleep(5000); // wait for seconds
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        List<String[]> data = m_executor.retrieveQueryResult(null, 1, 2);
        Assert.assertEquals(2, data.size());

        m_executor.stop(null);
    }

    // -- TODO: end to end testing. We can use TestAvroSystemFactory
    /* @Test
    public void testRetrieveQueryResult() {

    }

    @Test
    public void testConsumeQueryResult() {

    }

    @Test
    public void testExecuteQuery() {

    }

    @Test
    public void testGetRowCount() {

    }

    @Test
    public void testExecuteNonQuery() {

    }

    @Test
    public void testRemoveExecution() {

    }

    @Test
    public void queryExecutionStatus() {

    }*/
}
