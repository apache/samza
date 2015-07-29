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

package org.apache.samza.system.elasticsearch;

import org.apache.samza.metrics.*;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class ElasticsearchSystemProducerMetricsTest {

    public final static String GRP_NAME = "org.apache.samza.system.elasticsearch.ElasticsearchSystemProducerMetrics";

    @Test
    public void testMetrics() {
        ReadableMetricsRegistry registry = new MetricsRegistryMap();
        ElasticsearchSystemProducerMetrics metrics = new ElasticsearchSystemProducerMetrics("es", registry);
        metrics.bulkSendSuccess.inc(29L);
        metrics.inserts.inc();
        metrics.updates.inc(7L);
        metrics.conflicts.inc(3L);

        Set<String> groups = registry.getGroups();
        assertEquals(1, groups.size());
        assertEquals(GRP_NAME, groups.toArray()[0]);

        Map<String, Metric> metricMap = registry.getGroup(GRP_NAME);
        assertEquals(4, metricMap.size());
        assertEquals(29L, ((Counter) metricMap.get("es-bulk-send-success")).getCount());
        assertEquals(1L, ((Counter) metricMap.get("es-docs-inserted")).getCount());
        assertEquals(7L, ((Counter) metricMap.get("es-docs-updated")).getCount());
        assertEquals(3L, ((Counter) metricMap.get("es-version-conflicts")).getCount());
    }

}