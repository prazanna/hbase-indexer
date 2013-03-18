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
package com.ngdata.hbaseindexer.indexer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.List;

import com.ngdata.hbaseindexer.parse.HBaseToSolrMapper;
import org.apache.solr.common.SolrInputDocument;

import org.apache.hadoop.hbase.KeyValue.Type;

import com.google.common.collect.Lists;
import com.ngdata.hbaseindexer.indexer.Indexer.RowBasedIndexer;
import com.ngdata.hbaseindexer.conf.IndexConf;
import com.ngdata.hbaseindexer.conf.IndexConf.MappingType;
import com.ngdata.hbaseindexer.conf.IndexConfBuilder;
import com.ngdata.sep.SepEvent;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTablePool;
import org.junit.Before;
import org.junit.Test;

public class RowBasedIndexerTest {
    
    private static final String TABLE_NAME = "TABLE_A";
    
    private HTablePool tablePool;
    private SolrWriter solrWriter;
    private SolrUpdateCollector updateCollector;
    private RowBasedIndexer indexer;
    
    @Before
    public void setUp() {
        
        IndexConf indexConf = new IndexConfBuilder().table(TABLE_NAME).mappingType(MappingType.ROW).create();
        HBaseToSolrMapper mapper = IndexerTest.createHbaseToSolrMapper(true);
        
        tablePool = mock(HTablePool.class);
        solrWriter = mock(SolrWriter.class);
        
        updateCollector = new SolrUpdateCollector(10);
        
        indexer = new RowBasedIndexer("row-based", indexConf, mapper, tablePool, solrWriter);
    }
    
    private SepEvent createSepEvent(String row, KeyValue... keyValues) {
        return new SepEvent(TABLE_NAME.getBytes(), row.getBytes(), Lists.newArrayList(keyValues), null);
    }

    @Test
    public void testCalculateIndexUpdates_AddDocument() throws IOException {
        
        KeyValue keyValue = new KeyValue("_row_".getBytes(), "_cf_".getBytes(), "_qual_".getBytes(), "value".getBytes());
        SepEvent sepEvent = createSepEvent("_row_", keyValue);
        indexer.calculateIndexUpdates(Lists.newArrayList(sepEvent), updateCollector);
        
        assertEquals(1, updateCollector.getDocumentsToAdd().size());
        assertEquals("_row_", updateCollector.getDocumentsToAdd().get(0).getFieldValue("id"));
        assertTrue(updateCollector.getIdsToDelete().isEmpty());
    }
    
    @Test
    public void testCalculateIndexUpdates_Delete() throws IOException {
        
        KeyValue keyValue = new KeyValue("_row_".getBytes(), "_cf_".getBytes(), "_qual_".getBytes(), 0L, Type.Delete);
        SepEvent sepEvent = createSepEvent("_row_", keyValue);
        indexer.calculateIndexUpdates(Lists.newArrayList(sepEvent), updateCollector);
        
        assertEquals(Lists.newArrayList("_row_"), updateCollector.getIdsToDelete());
        assertTrue(updateCollector.getDocumentsToAdd().isEmpty());
    }
    
    @Test
    public void testCalculateIndexUpdates_UpdateAndDeleteCombinedForSameCell_DeleteFirst() throws IOException {
        KeyValue toDelete = new KeyValue("_row_".getBytes(), "_cf_".getBytes(), "_qual_".getBytes(), 0L, Type.Delete);
        KeyValue toAdd = new KeyValue("_row_".getBytes(), "_cf_".getBytes(), "_qual_".getBytes(), "value".getBytes());
        SepEvent deleteEvent = createSepEvent("_row_", toDelete);
        SepEvent addEvent = createSepEvent("_row_", toAdd);

        indexer.calculateIndexUpdates(Lists.newArrayList(deleteEvent, addEvent), updateCollector);

        assertTrue(updateCollector.getIdsToDelete().isEmpty());
        List<SolrInputDocument> documents = updateCollector.getDocumentsToAdd();
        assertEquals(1, documents.size());
        assertEquals("_row_", documents.get(0).getFieldValue("id"));
    }

    @Test
    public void testCalculateIndexUpdates_UpdateAndDeleteCombinedForSameCell_UpdateFirst() throws IOException {
        KeyValue toAdd = new KeyValue("_row_".getBytes(), "_cf_".getBytes(), "_qual_".getBytes(), "value".getBytes());
        KeyValue toDelete = new KeyValue("_row_".getBytes(), "_cf_".getBytes(), "_qual_".getBytes(), 0L, Type.Delete);
        SepEvent addEvent = createSepEvent("_row_", toAdd);
        SepEvent deleteEvent = createSepEvent("_row_", toDelete);

        indexer.calculateIndexUpdates(Lists.newArrayList(addEvent, deleteEvent), updateCollector);

        assertEquals(Lists.newArrayList("_row_"), updateCollector.getIdsToDelete());
        assertTrue(updateCollector.getDocumentsToAdd().isEmpty());
    }

}
