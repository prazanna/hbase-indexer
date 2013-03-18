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

    public static final String METRIC_GROUP = "hbaseindexer";

    public static void shutdownMetrics(Class<?> metricsOwnerClass, String indexerName) {
        SortedMap<String, SortedMap<MetricName, Metric>> groupedMetrics = Metrics.defaultRegistry().groupedMetrics(
                new IndexerMetricPredicate(indexerName));
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

        private String indexerName;

        public IndexerMetricPredicate(String indexerName) {
            this.indexerName = indexerName;
        }

        @Override
        public boolean matches(MetricName metricName, Metric metric) {
            return METRIC_GROUP.equals(metricName.getGroup()) && indexerName.equals(metricName.getScope());
        }

    }

    /**
     * Create a {@code MetricName} instance for a specific indexer.
     * 
     * @param producerClass class producing the metric
     * @param metric name of the metric
     * @param indexerName name of the indexer
     * @return {@code MetricName} instance scoped to the given indexer name
     */
    public static MetricName metricName(Class<?> producerClass, String metric, String indexerName) {
        return new MetricName("hbaseindexer", producerClass.getSimpleName(), metric, indexerName);
    }
}
