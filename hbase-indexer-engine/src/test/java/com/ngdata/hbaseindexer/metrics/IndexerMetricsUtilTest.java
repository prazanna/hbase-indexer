/*
 * Copyright 2013 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ngdata.hbaseindexer.metrics;

import static org.junit.Assert.assertEquals;

import com.yammer.metrics.core.MetricName;
import org.junit.Test;

public class IndexerMetricsUtilTest {

    @Test
    public void testMetricName() {
        String metric = "_metric_";
        String indexerName = "_index_name_";
        MetricName metricName = IndexerMetricsUtil.metricName(getClass(), metric, indexerName);
        
        assertEquals("hbaseindexer", metricName.getGroup());
        assertEquals("IndexerMetricsUtilTest", metricName.getType());
        assertEquals(metric, metricName.getName());
        assertEquals(indexerName, metricName.getScope());
    }
}
