package com.ngdata.hbaseindexer.impl;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.ngdata.hbaseindexer.model.api.ActiveBatchBuildInfoBuilder;
import com.ngdata.hbaseindexer.model.api.BatchBuildInfoBuilder;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerDefinitionBuilder;
import com.ngdata.hbaseindexer.model.impl.IndexerDefinitionJsonSerDeser;
import org.junit.Test;

import static com.ngdata.hbaseindexer.model.api.IndexerDefinition.BatchIndexingState;
import static com.ngdata.hbaseindexer.model.api.IndexerDefinition.IncrementalIndexingState;
import static com.ngdata.hbaseindexer.model.api.IndexerDefinition.LifecycleState;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class IndexerDefinitionJsonSerDeserTest {
    @Test
    public void testMinimal() {
        IndexerDefinition indexer = new IndexerDefinitionBuilder()
                .name("index1").build();

        IndexerDefinitionJsonSerDeser serdeser = new IndexerDefinitionJsonSerDeser();
        byte[] json = serdeser.toJsonBytes(indexer);

        IndexerDefinition indexer2 = serdeser.fromJsonBytes(json).build();

        assertEquals(indexer, indexer2);
        assertEquals("index1", indexer.getName());
    }

    @Test
    public void testFull() {
        IndexerDefinition indexer = new IndexerDefinitionBuilder()
                .name("index1")
                .lifecycleState(LifecycleState.DELETE_REQUESTED)
                .batchIndexingState(BatchIndexingState.BUILDING)
                .incrementalIndexingState(IncrementalIndexingState.SUBSCRIBE_DO_NOT_CONSUME)
                .configuration("config1".getBytes())
                .connectionType("solr")
                .connectionParams(ImmutableMap.of("p1", "v1", "p2", "v2"))
                .subscriptionId("my-subscription")
                .subscriptionTimestamp(5L)
                .defaultBatchIndexConfiguration("batch-conf-default".getBytes())
                .batchIndexConfiguration("batch-conf-next".getBytes())
                .activeBatchBuildInfo(new ActiveBatchBuildInfoBuilder()
                        .jobId("job-id-1")
                        .submitTime(10L)
                        .trackingUrl("url-1")
                        .batchIndexConfiguration("batch-conf-1".getBytes())
                        .build())
                .lastBatchBuildInfo(new BatchBuildInfoBuilder()
                        .jobId("job-id-2")
                        .submitTime(11L)
                        .trackingUrl("url-2")
                        .batchIndexConfiguration("batch-conf-2".getBytes())
                        .success(true)
                        .jobState("some-state")
                        .counter("counter-1", 1)
                        .counter("counter-2", 2)
                        .build())
                .occVersion(5).build();

        IndexerDefinitionJsonSerDeser serdeser = new IndexerDefinitionJsonSerDeser();
        byte[] json = serdeser.toJsonBytes(indexer);

        IndexerDefinition indexer2 = serdeser.fromJsonBytes(json).build();

        assertEquals("index1", indexer2.getName());
        assertEquals(LifecycleState.DELETE_REQUESTED, indexer2.getLifecycleState());
        assertEquals(BatchIndexingState.BUILDING, indexer2.getBatchIndexingState());
        assertEquals(IncrementalIndexingState.SUBSCRIBE_DO_NOT_CONSUME, indexer2.getIncrementalIndexingState());
        assertArrayEquals("config1".getBytes(), indexer2.getConfiguration());
        assertEquals("solr", indexer.getConnectionType());
        assertEquals("v1", indexer.getConnectionParams().get("p1"));
        assertEquals("v2", indexer.getConnectionParams().get("p2"));
        assertEquals("my-subscription", indexer2.getSubscriptionId());
        assertEquals(5L, indexer2.getSubscriptionTimestamp());
        assertArrayEquals("batch-conf-default".getBytes(), indexer2.getDefaultBatchIndexConfiguration());
        assertArrayEquals("batch-conf-next".getBytes(), indexer2.getBatchIndexConfiguration());

        assertNotNull(indexer2.getActiveBatchBuildInfo());
        assertEquals("job-id-1", indexer2.getActiveBatchBuildInfo().getJobId());
        assertEquals(10L, indexer2.getActiveBatchBuildInfo().getSubmitTime());
        assertEquals("url-1", indexer2.getActiveBatchBuildInfo().getTrackingUrl());
        assertArrayEquals("batch-conf-1".getBytes(), indexer2.getActiveBatchBuildInfo().getBatchIndexConfiguration());

        assertNotNull(indexer2.getLastBatchBuildInfo());
        assertEquals("job-id-2", indexer2.getLastBatchBuildInfo().getJobId());
        assertEquals(11L, indexer2.getLastBatchBuildInfo().getSubmitTime());
        assertEquals("url-2", indexer2.getLastBatchBuildInfo().getTrackingUrl());
        assertArrayEquals("batch-conf-2".getBytes(), indexer2.getLastBatchBuildInfo().getBatchIndexConfiguration());
        assertEquals(true, indexer2.getLastBatchBuildInfo().getSuccess());
        assertEquals("some-state", indexer2.getLastBatchBuildInfo().getJobState());
        assertEquals(Long.valueOf(1L), indexer2.getLastBatchBuildInfo().getCounters().get("counter-1"));
        assertEquals(Long.valueOf(2L), indexer2.getLastBatchBuildInfo().getCounters().get("counter-2"));

        assertEquals(5, indexer2.getOccVersion());

        assertEquals(indexer, indexer2);
    }
}
