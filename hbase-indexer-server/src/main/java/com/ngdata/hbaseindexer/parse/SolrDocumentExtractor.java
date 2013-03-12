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
package com.ngdata.hbaseindexer.parse;

import org.apache.hadoop.hbase.client.Result;
import org.apache.solr.common.SolrInputDocument;

/**
 * Extracts a value or collection of values from an HBase {@code Result} and transforms them into a {@code SolrInputDocument}.
 */
public interface SolrDocumentExtractor {

    /**
     * Extracts fields and values from an HBase {@code Result} and puts them into a SolrInputDocument.
     * 
     * @param input {@code Result} to be mapped to an indexable form
     * @param solrInputDocument document where fields are to be added
     */
    void extractDocument(Result input, SolrInputDocument solrInputDocument);
}
