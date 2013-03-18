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

import java.util.SortedMap;

import com.yammer.metrics.Metrics;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;

import com.yammer.metrics.core.MetricPredicate;

public class IndexerMetricsUtil {
    
    
    public static void shutdownMetrics(Class<?> metricsOwnerClass, String indexerName) {
        SortedMap<String, SortedMap<MetricName, Metric>> groupedMetrics = Metrics.defaultRegistry().groupedMetrics(new IndexerMetricPredicate(metricsOwnerClass, indexerName));
        for (SortedMap<MetricName, Metric> metricMap : groupedMetrics.values()) {
            for (MetricName metricName : metricMap.keySet()) {
                Metrics.defaultRegistry().removeMetric(metricName);
            }
        }
    }
    
    /**
     * MetricPredicate that matches all metrics created by a given class for a given indexer name.
     */
    static class IndexerMetricPredicate implements MetricPredicate {

        private String group;
        private String scope;

        public IndexerMetricPredicate(Class<?> ownerClass, String indexerName) {
            MetricName metricName = new MetricName(ownerClass, "Dummy", indexerName);
            this.group = metricName.getGroup();
            this.scope = metricName.getScope();
        }

        @Override
        public boolean matches(MetricName name, Metric metric) {
            return group.equals(name.getGroup()) && scope.equals(name.getScope());
        }

    }
}
