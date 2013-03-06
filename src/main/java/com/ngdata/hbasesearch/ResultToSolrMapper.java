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
package com.ngdata.hbasesearch;

import java.util.List;

import com.google.common.collect.Lists;
import com.ngdata.hbasesearch.conf.DocumentExtractDefinition;
import com.ngdata.hbasesearch.conf.FieldDefinition;
import com.ngdata.hbasesearch.parse.ByteArrayExtractor;
import com.ngdata.hbasesearch.parse.ByteArrayValueMapper;
import com.ngdata.hbasesearch.parse.ByteArrayValueMappers;
import com.ngdata.hbasesearch.parse.HBaseSolrDocumentExtractor;
import com.ngdata.hbasesearch.parse.SolrDocumentExtractor;
import com.ngdata.hbasesearch.parse.SolrInputDocumentBuilder;
import com.ngdata.hbasesearch.parse.TikaSolrDocumentExtractor;
import com.ngdata.hbasesearch.parse.extract.ByteArrayExtractors;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.solr.common.SolrInputDocument;

/**
 * Parses HBase {@code Result} objects into a structure of fields and values.
 */
public class ResultToSolrMapper implements HBaseToSolrMapper {

    /**
     * Map of Solr field names to transformers for extracting data from HBase {@code Result} objects.
     */
    private List<SolrDocumentExtractor> resultDocumentExtractors;

    /**
     * Get to be used for fetching field required for indexing.
     */
    private Get get;

    /**
     * Used to do evaluation on applicability of KeyValues.
     */
    private List<ByteArrayExtractor> extractors;

    /**
     * Instantiate based on a collection of {@link FieldDefinition}s.
     * 
     * @param fieldDefinitions define fields to be indexed
     */
    public ResultToSolrMapper(List<FieldDefinition> fieldDefinitions,
            List<DocumentExtractDefinition> documentExtractDefinitions) {
        get = new Get();
        extractors = Lists.newArrayList();
        resultDocumentExtractors = Lists.newArrayList();
        for (FieldDefinition fieldDefinition : fieldDefinitions) {
            ByteArrayExtractor byteArrayExtractor = ByteArrayExtractors.getExtractor(
                    fieldDefinition.getValueExpression(), fieldDefinition.getValueSource());
            ByteArrayValueMapper valueMapper = ByteArrayValueMappers.getMapper(fieldDefinition.getTypeName());
            resultDocumentExtractors.add(new HBaseSolrDocumentExtractor(fieldDefinition.getName(), byteArrayExtractor,
                    valueMapper));
            extractors.add(byteArrayExtractor);
        }

        for (DocumentExtractDefinition extractDefinition : documentExtractDefinitions) {
            ByteArrayExtractor byteArrayExtractor = ByteArrayExtractors.getExtractor(
                    extractDefinition.getValueExpression(), extractDefinition.getValueSource());
            resultDocumentExtractors.add(TikaSolrDocumentExtractor.createInstance(byteArrayExtractor,
                    extractDefinition.getPrefix(), extractDefinition.getMimeType()));
            extractors.add(byteArrayExtractor);
        }

        for (ByteArrayExtractor extractor : extractors) {

            byte[] columnFamily = extractor.getColumnFamily();
            byte[] columnQualifier = extractor.getColumnQualifier();
            if (columnFamily != null) {
                if (columnQualifier != null) {
                    get.addColumn(columnFamily, columnQualifier);
                } else {
                    get.addFamily(columnFamily);
                }
            }

        }
    }

    @Override
    public boolean isRelevantKV(KeyValue kv) {
        for (ByteArrayExtractor extractor : extractors) {
            if (extractor.isApplicable(kv)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Get getGet(byte[] row) {
        return get;
    }

    @Override
    public SolrInputDocument map(Result result) {
        SolrInputDocumentBuilder solrDocumentBuilder = new SolrInputDocumentBuilder();
        for (SolrDocumentExtractor documentExtractor : resultDocumentExtractors) {
            solrDocumentBuilder.add(documentExtractor.extractDocument(result));
        }
        return solrDocumentBuilder.getDocument();
    }

}
