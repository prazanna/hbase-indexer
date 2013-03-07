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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;

import com.google.common.collect.Lists;
import com.ngdata.hbaseindexer.conf.DocumentExtractDefinition;
import com.ngdata.hbaseindexer.conf.FieldDefinition;
import com.ngdata.hbaseindexer.parse.ByteArrayExtractor;
import com.ngdata.hbaseindexer.parse.ByteArrayValueMapper;
import com.ngdata.hbaseindexer.parse.ByteArrayValueMappers;
import com.ngdata.hbaseindexer.parse.HBaseSolrDocumentExtractor;
import com.ngdata.hbaseindexer.parse.SolrDocumentExtractor;
import com.ngdata.hbaseindexer.parse.TikaSolrDocumentExtractor;
import com.ngdata.hbaseindexer.parse.extract.ByteArrayExtractors;
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
     * Information to be used for constructing a Get to fetch data required for indexing.
     */
    private Map<byte[], NavigableSet<byte[]>> familyMap;

    /**
     * Used to do evaluation on applicability of KeyValues.
     */
    private List<ByteArrayExtractor> extractors;
    
    
    /**
     * Instantiate with {@code FieldDefinition}s.
     * 
     * @param fieldDefinitions
     */
    public ResultToSolrMapper(List<FieldDefinition> fieldDefinitions) {
        this(fieldDefinitions, Collections.<DocumentExtractDefinition>emptyList());
    }

    /**
     * Instantiate with {@code FieldDefinitions}s and {@code DocumentExtractDefinition}s.
     * 
     * @param fieldDefinitions define fields to be indexed
     * @param documentExtractDefinitions additional document extraction definitions
     */
    public ResultToSolrMapper(List<FieldDefinition> fieldDefinitions,
            List<DocumentExtractDefinition> documentExtractDefinitions) {
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

        Get get = new Get();
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
        familyMap = get.getFamilyMap();
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
        Get get = new Get(row);
        for (Entry<byte[], NavigableSet<byte[]>> familyMapEntry : familyMap.entrySet()) {
            byte[] columnFamily = familyMapEntry.getKey();
            if (familyMapEntry.getValue() == null) {
                get.addFamily(columnFamily);
            } else {
                for (byte[] qualifier : familyMapEntry.getValue()) {
                    get.addColumn(columnFamily, qualifier);
                }
            }
        }
        return get;
    }

    @Override
    public SolrInputDocument map(Result result) {
        SolrInputDocument solrInputDocument = new SolrInputDocument();
        for (SolrDocumentExtractor documentExtractor : resultDocumentExtractors) {
            documentExtractor.extractDocument(result, solrInputDocument);
        }
        return solrInputDocument;
    }

}
