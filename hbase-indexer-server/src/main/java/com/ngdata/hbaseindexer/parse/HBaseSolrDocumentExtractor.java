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

import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Result;
import org.apache.solr.common.SolrInputDocument;

/**
 * Extracts a {@code SolrInputDocument} from an HBase {@code Result} object.
 */
public class HBaseSolrDocumentExtractor implements SolrDocumentExtractor {

    private String fieldName;
    private ByteArrayExtractor valueExtractor;
    private ByteArrayValueMapper valueMapper;

    public HBaseSolrDocumentExtractor(String fieldName, ByteArrayExtractor valueExtractor,
            ByteArrayValueMapper valueMapper) {
        this.fieldName = fieldName;
        this.valueExtractor = valueExtractor;
        this.valueMapper = valueMapper;
    }

    /**
     * Extracts byte arrays from the given {@code Result}, and transforms them into a {@code SolrInputDocument}.
     * 
     * @param result source of byte array data
     * @param solrInputDocument document where indexable data is to be added
     */
    @Override
    public void extractDocument(Result result, SolrInputDocument solrInputDocument) {
        List<Object> values = Lists.newArrayList();
        for (byte[] bytes : valueExtractor.extract(result)) {
            values.addAll(valueMapper.map(bytes));
        }
        solrInputDocument.addField(fieldName, values);
    }

}
