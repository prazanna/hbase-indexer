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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.ngdata.hbaseindexer.conf.IndexerConf;
import com.ngdata.hbaseindexer.conf.IndexerConfBuilder;
import com.ngdata.hbaseindexer.indexer.Indexer.ColumnBasedIndexer;
import com.ngdata.hbaseindexer.parse.ResultToSolrMapper;
import com.ngdata.sep.SepEvent;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.Test;

public class ColumnBasedIndexerTest {

    private static final String TABLE_NAME = "TABLE_A";

    private IndexerConf indexerConf;
    private ResultToSolrMapper mapper;
    private SolrWriter solrWriter;
    private SolrUpdateCollector updateCollector;
    private ColumnBasedIndexer indexer;

    @Before
    public void setUp() {
        indexerConf = spy(new IndexerConfBuilder().table(TABLE_NAME).build());
        mapper = IndexerTest.createHbaseToSolrMapper(true);
        solrWriter = mock(SolrWriter.class);
        updateCollector = new SolrUpdateCollector(10);
        indexer = new ColumnBasedIndexer("column-based", indexerConf, mapper, solrWriter);
    }

    private SepEvent createSepEvent(String row, KeyValue... keyValues) {
        return new SepEvent(TABLE_NAME.getBytes(), row.getBytes(), Lists.newArrayList(keyValues), null);
    }

    @Test
    public void testCalculateIndexUpdates_AddDocument() throws IOException {
        KeyValue toAdd = new KeyValue("_row_".getBytes(), "_cf_".getBytes(), "_qual_".getBytes(), "value".getBytes());
        SepEvent event = createSepEvent("_row_", toAdd);

        indexer.calculateIndexUpdates(Lists.newArrayList(event), updateCollector);

        assertTrue(updateCollector.getIdsToDelete().isEmpty());
        List<SolrInputDocument> documents = updateCollector.getDocumentsToAdd();
        assertEquals(1, documents.size());
        assertEquals("_row_-_cf_-_qual_", documents.get(0).getFieldValue("id"));
    }

    @Test
    public void testCalculateIndexUpdates_DeleteDocument() throws IOException {
        KeyValue toDelete = new KeyValue("_row_".getBytes(), "_cf_".getBytes(), "_qual_".getBytes(), 0L, Type.DeleteColumn);
        SepEvent event = createSepEvent("_row_", toDelete);

        indexer.calculateIndexUpdates(Lists.newArrayList(event), updateCollector);

        assertEquals(Lists.newArrayList("_row_-_cf_-_qual_"), updateCollector.getIdsToDelete());
        assertTrue(updateCollector.getDocumentsToAdd().isEmpty());
        assertTrue(updateCollector.getDeleteQueries().isEmpty());
    }
    
    @Test
    public void testCalculateIndexUpdates_DeleteFamily() throws IOException {

        final String ROW_FIELD = "_row_field_";
        final String FAMILY_FIELD = "_family_field_";

        doReturn(ROW_FIELD).when(indexerConf).getRowField();
        doReturn(FAMILY_FIELD).when(indexerConf).getColumnFamilyField();

        KeyValue toDelete = new KeyValue("_row_".getBytes(), "_cf_".getBytes(), "_qual_".getBytes(), 0L,
                Type.DeleteFamily);
        SepEvent event = createSepEvent("_row_", toDelete);

        indexer.calculateIndexUpdates(Lists.newArrayList(event), updateCollector);

        assertEquals(ImmutableList.of("(" + ROW_FIELD + ":_row_)AND(" + FAMILY_FIELD + ":_cf_)"),
                updateCollector.getDeleteQueries());
        assertTrue(updateCollector.getIdsToDelete().isEmpty());
        assertTrue(updateCollector.getDocumentsToAdd().isEmpty());
    }
    
    @Test
    public void testCalculateIndexUpdates_DeleteRow() throws IOException {
        final String ROW_FIELD = "_row_field_";
        
        doReturn(ROW_FIELD).when(indexerConf).getRowField();
        
        KeyValue toDelete = new KeyValue("_row_".getBytes(), "_cf_".getBytes(), "_qual_".getBytes(), 0L, Type.Delete);
        SepEvent event = createSepEvent("_row_", toDelete);
        
        indexer.calculateIndexUpdates(Lists.newArrayList(event), updateCollector);

        assertEquals(ImmutableList.of(ROW_FIELD + ":_row_"), updateCollector.getDeleteQueries());
        assertTrue(updateCollector.getIdsToDelete().isEmpty());
        assertTrue(updateCollector.getDocumentsToAdd().isEmpty());
    }

    // Deleting by family can only work when a family field is defined in the indexer conf.
    @Test
    public void testCalculateIndexUpdates_DeleteFamily_NoFamilyFieldDefinedForIndexer() throws IOException {
        KeyValue toDelete = new KeyValue("_row_".getBytes(), "_cf_".getBytes(), "_qual_".getBytes(), 0L, Type.DeleteFamily);
        SepEvent event = createSepEvent("_row_", toDelete);
        
        indexer.calculateIndexUpdates(Lists.newArrayList(event), updateCollector);

        assertTrue(updateCollector.getDeleteQueries().isEmpty());
        assertTrue(updateCollector.getIdsToDelete().isEmpty());
        assertTrue(updateCollector.getDocumentsToAdd().isEmpty());
    }
    
    // Deleting all values for a row can only work when a row field is defined in the indexer conf
    @Test
    public void testCalculateIndexUpdates_DeleteRow_NoRowFieldDefinedForIndexer() throws IOException {
        KeyValue toDelete = new KeyValue("_row_".getBytes(), "_cf_".getBytes(), "_qual_".getBytes(), 0L, Type.Delete);
        SepEvent event = createSepEvent("_row_", toDelete);
        
        indexer.calculateIndexUpdates(Lists.newArrayList(event), updateCollector);

        assertTrue(updateCollector.getDeleteQueries().isEmpty());
        assertTrue(updateCollector.getIdsToDelete().isEmpty());
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
        assertEquals("_row_-_cf_-_qual_", documents.get(0).getFieldValue("id"));
    }

    @Test
    public void testCalculateIndexUpdates_UpdateAndDeleteCombinedForSameCell_UpdateFirst() throws IOException {
        KeyValue toAdd = new KeyValue("_row_".getBytes(), "_cf_".getBytes(), "_qual_".getBytes(), "value".getBytes());
        KeyValue toDelete = new KeyValue("_row_".getBytes(), "_cf_".getBytes(), "_qual_".getBytes(), 0L, Type.DeleteColumn);
        SepEvent addEvent = createSepEvent("_row_", toAdd);
        SepEvent deleteEvent = createSepEvent("_row_", toDelete);

        indexer.calculateIndexUpdates(Lists.newArrayList(addEvent, deleteEvent), updateCollector);

        assertEquals(Lists.newArrayList("_row_-_cf_-_qual_"), updateCollector.getIdsToDelete());
        assertTrue(updateCollector.getDocumentsToAdd().isEmpty());
    }
    
    @Test
    public void testCalculateIndexUpdates_WithRowFieldDefined() throws IOException {
        final String CUSTOM_ROW_FIELD = "custom-row-field";
        doReturn(CUSTOM_ROW_FIELD).when(indexerConf).getRowField();
        
        KeyValue toAdd = new KeyValue("_row_".getBytes(), "_cf_".getBytes(), "_qual_".getBytes(), "value".getBytes());
        SepEvent event = createSepEvent("_row_", toAdd);

        indexer.calculateIndexUpdates(Lists.newArrayList(event), updateCollector);

        assertTrue(updateCollector.getIdsToDelete().isEmpty());
        List<SolrInputDocument> documents = updateCollector.getDocumentsToAdd();
        assertEquals(1, documents.size());
        assertEquals("_row_-_cf_-_qual_", documents.get(0).getFieldValue("id"));
        assertEquals("_row_", documents.get(0).getFieldValue(CUSTOM_ROW_FIELD));
        
    }
    
    @Test
    public void testCalculateIndexUpdates_WithFamilyFieldDefined() throws IOException {
        final String CUSTOM_FAMILY_FIELD = "custom-row-field";
        doReturn(CUSTOM_FAMILY_FIELD).when(indexerConf).getColumnFamilyField();
        
        KeyValue toAdd = new KeyValue("_row_".getBytes(), "_cf_".getBytes(), "_qual_".getBytes(), "value".getBytes());
        SepEvent event = createSepEvent("_row_", toAdd);

        indexer.calculateIndexUpdates(Lists.newArrayList(event), updateCollector);

        assertTrue(updateCollector.getIdsToDelete().isEmpty());
        List<SolrInputDocument> documents = updateCollector.getDocumentsToAdd();
        assertEquals(1, documents.size());
        assertEquals("_row_-_cf_-_qual_", documents.get(0).getFieldValue("id"));
        assertEquals("_cf_", documents.get(0).getFieldValue(CUSTOM_FAMILY_FIELD));
    }
    

}
