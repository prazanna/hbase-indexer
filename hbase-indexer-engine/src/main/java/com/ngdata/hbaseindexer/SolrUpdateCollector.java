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
package com.ngdata.hbaseindexer;

import java.util.List;

import com.google.common.collect.Lists;

import org.apache.solr.common.SolrInputDocument;

/**
 * Collects updates to be passed on to Solr in batch.
 */
public class SolrUpdateCollector {

    private List<SolrInputDocument> documentsToAdd;

    private List<String> idsToDelete;
    
    /**
     * Instantiate with an expected initial capacity of added and deleted documents.
     */
    public SolrUpdateCollector(int initialSize) {
        documentsToAdd = Lists.newArrayListWithCapacity(initialSize);
        idsToDelete = Lists.newArrayListWithCapacity(initialSize);
    }

    /**
     * Add a new {@code SolrInputDocument} that will be later added to Solr in a batch update.
     * 
     * @param solrDocument document to be added
     */
    public void add(SolrInputDocument solrDocument) {
        documentsToAdd.add(solrDocument);
    }

    /**
     * Add a new document id that will be later deleted from Solr in a batch update.
     * 
     * @param documentId id of the document to be deleted
     */
    public void deleteById(String documentId) {
        idsToDelete.add(documentId);
    }

    /**
     * Get all documents to be added in batch.
     * 
     * @return the list of documents
     */
    public List<SolrInputDocument> getDocumentsToAdd() {
        return documentsToAdd;
    }

    /**
     * Get all ids of documents to be deleted in batch.
     * 
     * @return list of document ids
     */
    public List<String> getIdsToDelete() {
        return idsToDelete;
    }

}
