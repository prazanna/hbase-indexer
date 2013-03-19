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
package com.ngdata.hbaseindexer.parse.tika;

import java.io.ByteArrayInputStream;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.ngdata.hbaseindexer.Configurable;
import com.ngdata.hbaseindexer.parse.ByteArrayExtractor;
import com.ngdata.hbaseindexer.parse.SolrDocumentExtractor;
import com.ngdata.hbaseindexer.parse.SolrInputDocumentBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.handler.extraction.SolrContentHandler;
import org.apache.solr.schema.IndexSchema;
import org.apache.tika.detect.Detector;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;

/**
 * Tika-based document extractor.
 * <p>
 * This implementation has no knowledge of the input data structure, and hands off all processing to <a
 * href="http://tika.apache.org">Tika</a>.
 */
public class TikaSolrDocumentExtractor implements SolrDocumentExtractor, Configurable {
    
    Map<String, String> DEFAULT_CELL_PARAMS = ImmutableMap.of(
                                                "lowernames", "true",
                                                "fmap.content_encoding", "ignored_field",
                                                "fmap.content_type", "ignored_field");

    private IndexSchema indexSchema;
    private ByteArrayExtractor extractor;
    private String fieldNamePrefix;
    private String mimeType;
    private AutoDetectParser parser;
    private Map<String,String> params;

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
        this.fieldNamePrefix = fieldNamePrefix == null ? "" : fieldNamePrefix ;
        this.mimeType = mimeType;
        parser = new AutoDetectParser();
        Detector detector = new LiteralMimeDetector(parser.getDetector());
        parser.setDetector(detector);
    }

    @Override
    public void extractDocument(Result result, SolrInputDocument solrInputDocument) {
        SolrInputDocumentBuilder builder = new SolrInputDocumentBuilder(solrInputDocument);
        for (byte[] bytes : extractor.extract(result)) {
            builder.add(extractInternal(bytes), fieldNamePrefix);
        }
    }

    private SolrInputDocument extractInternal(byte[] input) {
        Metadata metadata = new Metadata();
        metadata.add(LiteralMimeDetector.MIME_TYPE, mimeType);

        Map<String, String> cellParams = (params == null || params.isEmpty()) ? DEFAULT_CELL_PARAMS : params;
        SolrContentHandler handler = new SolrContentHandler(metadata, new MapSolrParams(cellParams), indexSchema);

        try {
            parser.parse(new ByteArrayInputStream(input), handler, metadata, new ParseContext());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return handler.newDocument();
    }

    @Override
    public void configure(Map<String, String> params) {
        this.params = params;
    }

}
