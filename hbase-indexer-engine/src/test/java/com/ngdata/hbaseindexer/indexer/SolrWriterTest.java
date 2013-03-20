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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.Test;

public class SolrWriterTest {

    private SolrServer solrServer;
    private SolrWriter solrWriter;

    @Before
    public void setUp() {
        solrServer = mock(SolrServer.class);
        solrWriter = new SolrWriter("index name", solrServer);
    }

    @Test
    public void testAdd_NormalCase() throws SolrServerException, IOException {
        SolrInputDocument inputDocA = mock(SolrInputDocument.class);
        SolrInputDocument inputDocB = mock(SolrInputDocument.class);
        List<SolrInputDocument> toAdd = Lists.newArrayList(inputDocA, inputDocB);

        solrWriter.add(toAdd);

        verify(solrServer).add(toAdd);
    }

    @Test
    public void testDeleteById_NormalCase() throws SolrServerException, IOException {
        List<String> toDelete = Lists.newArrayList("idA", "idB");

        solrWriter.deleteById(toDelete);

        verify(solrServer).deleteById(toDelete);
    }

    @Test(expected = IOException.class)
    public void testAdd_IOException() throws SolrServerException, IOException {
        List<SolrInputDocument> inputDocs = Lists.<SolrInputDocument> newArrayList(mock(SolrInputDocument.class));

        when(solrServer.add(inputDocs)).thenThrow(new IOException());

        solrWriter.add(inputDocs);
    }

    @Test(expected = IOException.class)
    public void testDeleteById_IOException() throws SolrServerException, IOException {
        List<String> idsToDelete = Lists.newArrayList("idA", "idB");

        when(solrServer.deleteById(idsToDelete)).thenThrow(new IOException());

        solrWriter.deleteById(idsToDelete);
    }

    @Test(expected = SolrException.class)
    public void testAdd_SolrExceptionCausedByIOException() throws SolrServerException, IOException {
        List<SolrInputDocument> inputDocuments = Lists.<SolrInputDocument> newArrayList(mock(SolrInputDocument.class));

        when(solrServer.add(inputDocuments)).thenThrow(new SolrException(ErrorCode.SERVER_ERROR, new IOException()));

        solrWriter.add(inputDocuments);
    }

    @Test(expected = SolrException.class)
    public void testDeleteById_SolrExceptionCausedByIOException() throws SolrServerException, IOException {
        List<String> idsToDelete = Lists.newArrayList("idA", "idB");

        when(solrServer.deleteById(idsToDelete)).thenThrow(new SolrException(ErrorCode.SERVER_ERROR, new IOException()));

        solrWriter.deleteById(idsToDelete);
    }

    @Test
    public void testAdd_BadRequest() throws SolrServerException, IOException {
        List<SolrInputDocument> inputDocument = Lists.<SolrInputDocument> newArrayList(mock(SolrInputDocument.class));

        when(solrServer.add(inputDocument)).thenThrow(
                new SolrException(ErrorCode.BAD_REQUEST, "should be swallowed and logged"));

        solrWriter.add(inputDocument);

        // Nothing should happen -- no document successfully added, and exception is swallowed
    }

    @Test
    public void testDeleteById_BadRequest() throws SolrServerException, IOException {
        List<String> idsToDelete = Lists.newArrayList("idA", "idB");

        when(solrServer.deleteById(idsToDelete)).thenThrow(
                new SolrException(ErrorCode.BAD_REQUEST, "should be swallowed and logged"));

        solrWriter.deleteById(idsToDelete);

        // Nothing should happen -- no document successfully added, and exception is swallowed
    }
    
    @Test
    public void testAdd_RetryIndividually() throws SolrServerException, IOException {
        SolrInputDocument badInputDoc = mock(SolrInputDocument.class);
        SolrInputDocument goodInputDoc = mock(SolrInputDocument.class);
        
        List<SolrInputDocument> inputDocs = Lists.newArrayList(badInputDoc, goodInputDoc);
        
        when(solrServer.add(inputDocs)).thenThrow(new SolrException(ErrorCode.BAD_REQUEST, "bad document"));
        when(solrServer.add(badInputDoc)).thenThrow(new SolrException(ErrorCode.BAD_REQUEST, "bad document"));
        
        solrWriter.add(inputDocs);
        
        verify(solrServer).add(goodInputDoc);
    }
    
    @Test
    public void testDeleteById_RetryIndividually() throws SolrServerException, IOException {
        String badId = "badId";
        String goodId = "goodId";
        List<String> idsToDelete = Lists.newArrayList(badId, goodId);
        
        when(solrServer.deleteById(idsToDelete)).thenThrow(new SolrException(ErrorCode.BAD_REQUEST, "bad id"));
        when(solrServer.deleteById(badId)).thenThrow(new SolrException(ErrorCode.BAD_REQUEST, "bad id"));
        
        solrWriter.deleteById(idsToDelete);
        
        verify(solrServer).deleteById(goodId);
    }
    
    @Test
    public void testDeleteByQuery() throws SolrServerException, IOException {
        String deleteQuery = "_delete_query_";
        
        solrWriter.deleteByQuery(deleteQuery);
        
        verify(solrServer).deleteByQuery(deleteQuery);
    }

}
