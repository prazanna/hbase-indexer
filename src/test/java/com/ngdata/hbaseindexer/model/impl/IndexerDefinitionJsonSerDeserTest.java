package com.ngdata.hbaseindexer.model.impl;

import com.ngdata.hbaseindexer.model.api.ActiveBatchBuildInfoBuilder;
import com.ngdata.hbaseindexer.model.api.BatchBuildInfoBuilder;
import com.ngdata.hbaseindexer.model.api.IndexBatchBuildState;
import com.ngdata.hbaseindexer.model.api.IndexGeneralState;
import com.ngdata.hbaseindexer.model.api.IndexUpdateState;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerDefinitionBuilder;
import org.junit.Test;

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
                .generalState(IndexGeneralState.DELETE_REQUESTED)
                .batchBuildState(IndexBatchBuildState.BUILDING)
                .updateState(IndexUpdateState.SUBSCRIBE_DO_NOT_LISTEN)
                .configuration("config1".getBytes())
                .connectionConfiguration("config2".getBytes())
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
                .zkDataVersion(5).build();

        IndexerDefinitionJsonSerDeser serdeser = new IndexerDefinitionJsonSerDeser();
        byte[] json = serdeser.toJsonBytes(indexer);

        IndexerDefinition indexer2 = serdeser.fromJsonBytes(json).build();

        assertEquals("index1", indexer2.getName());
        assertEquals(IndexGeneralState.DELETE_REQUESTED, indexer2.getGeneralState());
        assertEquals(IndexBatchBuildState.BUILDING, indexer2.getBatchBuildState());
        assertEquals(IndexUpdateState.SUBSCRIBE_DO_NOT_LISTEN, indexer2.getUpdateState());
        assertArrayEquals("config1".getBytes(), indexer2.getConfiguration());
        assertArrayEquals("config2".getBytes(), indexer2.getConnectionConfiguration());
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

        assertEquals(5, indexer2.getZkDataVersion());

        assertEquals(indexer, indexer2);
    }
}
