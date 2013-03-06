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
package com.ngdata.hbasesearch.parse;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.handler.extraction.ExtractingMetadataConstants;
import org.apache.solr.handler.extraction.SolrContentHandler;
import org.apache.solr.schema.IndexSchema;
import org.apache.tika.metadata.HttpHeaders;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.xml.sax.InputSource;

/**
 * Tika-based document extractor.
 * <p>
 * This implementation has no knowledge of the input data structure, and hands off all processing to <a
 * href="http://tika.apache.org">Tika</a>.
 */
public class TikaSolrDocumentExtractor implements SolrDocumentExtractor {

    private IndexSchema indexSchema;
    private ByteArrayExtractor extractor;
    private String fieldNamePrefix;
    private String mimeType;
    private AutoDetectParser parser;

    /**
     * Instantiate with the mime type of the input that will be handled by this extractor.
     * 
     * @param indexSchema Solr indexing schema definition
     * @param extractor extracts byte arrays from HBase {@code Result}s
     * @param fieldNamePrefix prefix to be added to all Solr document field names
     * @param mimeType the mime type to be used as the default by Tika
     */
    public TikaSolrDocumentExtractor(IndexSchema indexSchema, ByteArrayExtractor extractor, String fieldNamePrefix,
            String mimeType) {
        this.indexSchema = indexSchema;
        this.extractor = extractor;
        this.fieldNamePrefix = fieldNamePrefix;
        this.mimeType = mimeType;
        parser = new AutoDetectParser();
    }

    @Override
    public SolrInputDocument extractDocument(Result result) {
        SolrInputDocumentBuilder builder = new SolrInputDocumentBuilder();
        for (byte[] bytes : extractor.extract(result)) {
            builder.add(extractInternal(bytes), fieldNamePrefix);
        }
        return builder.getDocument();
    }

    private SolrInputDocument extractInternal(byte[] input) {
        Metadata metadata = new Metadata();
        metadata.add(HttpHeaders.CONTENT_TYPE, mimeType);
        metadata.add(ExtractingMetadataConstants.STREAM_CONTENT_TYPE, mimeType);

        // TODO Need to check which parameters (if any) need to be given
        Map<String, String> cellParams = new HashMap<String, String>();
        SolrContentHandler handler = new SolrContentHandler(metadata, new MapSolrParams(cellParams), indexSchema);

        try {
            parser.parse(new ByteArrayInputStream(input), handler, metadata, new ParseContext());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return handler.newDocument();
    }

    /**
     * Factory method to create a {@code TikaSolrDocumentExtractor} based on a default Solr configuration.
     * 
     * @param extractor extracts byte arrays from HBase {@code Result}s
     * @param fieldNamePrefix prefix to be added to all Solr document field names
     * @param mimeType the mime type to be used as the default by Tika
     * @return a new extractor instance
     */
    public static TikaSolrDocumentExtractor createInstance(ByteArrayExtractor extractor, String fieldNamePrefix,
            String mimeType) {
        InputSource configInputSource = new InputSource(
                TikaSolrDocumentExtractor.class.getResourceAsStream("/solrconfig.xml"));
        SolrConfig solrConfig;
        try {
            solrConfig = new SolrConfig("example", configInputSource);
        } catch (Exception e) {
            throw new RuntimeException("Error while parsing default Solr configuration", e);
        }

        InputSource schemaInputSource = new InputSource(
                TikaSolrDocumentExtractor.class.getResourceAsStream("/schema.xml"));
        IndexSchema indexSchema = new IndexSchema(solrConfig, null, schemaInputSource);
        return new TikaSolrDocumentExtractor(indexSchema, extractor, fieldNamePrefix, mimeType);
    }

}
