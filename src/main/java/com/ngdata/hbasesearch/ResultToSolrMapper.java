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
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.TreeMultimap;

import com.google.common.collect.SetMultimap;

import com.google.common.collect.HashMultimap;

import com.google.common.collect.Multimaps;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.solr.common.SolrInputDocument;

import com.ngdata.hbasesearch.conf.FieldDefinition;

import com.ngdata.hbasesearch.parse.ByteArrayExtractor;
import com.ngdata.hbasesearch.parse.ByteArrayValueMapper;
import com.ngdata.hbasesearch.parse.ByteArrayValueMappers;
import com.ngdata.hbasesearch.parse.IndexValueTransformer;
import com.ngdata.hbasesearch.parse.ResultIndexValueTransformer;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.ngdata.hbasesearch.parse.extract.ByteArrayExtractors;
import org.apache.hadoop.hbase.client.Result;

/**
 * Parses HBase {@code Result} objects into a structure of fields and values.
 */
public class ResultToSolrMapper implements HBaseToSolrMapper {

    /**
     * Map of Solr field names to transformers for extracting data from HBase {@code Result} objects.
     */
    private List<IndexValueTransformer<Result>> valueTransformers;
    
    /**
     * Get to be used for fetching field required for indexing.
     */
    private Get get;
    

    /**
     * Instantiate based on a collection of {@link FieldDefinition}s.
     * @param fieldDefinitions define fields to be indexed
     */
    public ResultToSolrMapper(List<FieldDefinition> fieldDefinitions) {
        get = new Get();
        valueTransformers = Lists.newArrayList();
        for (FieldDefinition fieldDefinition : fieldDefinitions) {
            ByteArrayExtractor byteArrayExtractor = ByteArrayExtractors.getExtractor(
                    fieldDefinition.getValueExpression(), fieldDefinition.getValueSource());
            ByteArrayValueMapper valueMapper = ByteArrayValueMappers.getMapper(fieldDefinition.getTypeName());
            valueTransformers.add(new ResultIndexValueTransformer(fieldDefinition.getName(), byteArrayExtractor,
                    valueMapper));
            
            byte[] columnFamily = byteArrayExtractor.getColumnFamily();
            byte[] columnQualifier = byteArrayExtractor.getColumnQualifier();
            if (columnFamily != null) {
                if (columnQualifier != null) {
                    get.addColumn(columnFamily, columnQualifier);
                } else {
                    get.addFamily(columnFamily);
                }
            }

        }
    }

    public Multimap<String, Object> parse(Result result) {
        Multimap<String, Object> parsedValueMap = ArrayListMultimap.create();
        for (IndexValueTransformer<Result> valueTransformer : valueTransformers) {
            parsedValueMap.putAll(valueTransformer.extractAndTransform(result));
        }
        return parsedValueMap;
    }

    @Override
    public boolean isRelevantKV(KeyValue kv) {
        // TODO Use the ordering of the family map to make this much more efficient
        Map<byte[], NavigableSet<byte[]>> familyMap = get.getFamilyMap();
        for (Entry<byte[], NavigableSet<byte[]>> entry : familyMap.entrySet()) {
            if (kv.matchingFamily(entry.getKey())) {
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
        Multimap<String, Object> parsedMultimap = parse(result);
        SolrInputDocument solrInputDocument = new SolrInputDocument();
        for (String fieldName : parsedMultimap.keySet()) {
            for (Object fieldValue : parsedMultimap.get(fieldName)) {
                solrInputDocument.addField(fieldName, fieldValue);
            }
        }
        return solrInputDocument;
    }

}
