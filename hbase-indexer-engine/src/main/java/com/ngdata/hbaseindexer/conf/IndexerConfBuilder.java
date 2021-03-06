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
package com.ngdata.hbaseindexer.conf;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.ngdata.hbaseindexer.conf.FieldDefinition.ValueSource;
import com.ngdata.hbaseindexer.parse.ResultToSolrMapper;
import com.ngdata.hbaseindexer.uniquekey.UniqueKeyFormatter;

/**
 * A builder for creating {@link IndexerConf} instances.
 */
public class IndexerConfBuilder {
    private String table;
    private String uniqueKeyField;
    private String rowField;
    private String columnFamilyField;
    private Class<? extends ResultToSolrMapper> mapperClass;
    private Class<? extends UniqueKeyFormatter> uniqueKeyFormatterClass;
    private IndexerConf.RowReadMode rowReadMode = IndexerConf.RowReadMode.DYNAMIC;
    private IndexerConf.MappingType mappingType = IndexerConf.MappingType.ROW;
    private List<FieldDefinition> fieldDefinitions = Lists.newArrayList();
    private List<DocumentExtractDefinition> documentExtractDefinitions = Lists.newArrayList();
    private Map<String,String> globalParams;

    public IndexerConfBuilder table(String table) {
        this.table = table;
        return this;
    }

    public IndexerConfBuilder mappingType(IndexerConf.MappingType mappingType) {
        this.mappingType = mappingType;
        return this;
    }

    public IndexerConfBuilder rowReadMode(IndexerConf.RowReadMode rowReadMode) {
        this.rowReadMode = rowReadMode;
        return this;
    }

    public IndexerConfBuilder uniqueyKeyField(String uniqueKeyField) {
        this.uniqueKeyField = uniqueKeyField;
        return this;
    }
    
    public IndexerConfBuilder rowField(String rowField) {
        this.rowField = rowField;
        return this;
    }
    
    public IndexerConfBuilder columnFamilyField(String columnFamilyField) {
        this.columnFamilyField = columnFamilyField;
        return this;
    }
    
    public IndexerConfBuilder mapperClass(Class<? extends ResultToSolrMapper> mapperClass) {
        this.mapperClass = mapperClass;
        return this;
    }

    public IndexerConfBuilder uniqueKeyFormatterClass(Class<? extends UniqueKeyFormatter> uniqueKeyFormatterClass) {
        this.uniqueKeyFormatterClass = uniqueKeyFormatterClass;
        return this;
    }
    
    public IndexerConfBuilder globalParams(Map<String,String> globalParams) {
        this.globalParams = globalParams;
        return this;
    }

    public IndexerConfBuilder addFieldDefinition(String name, String valueExpression,
            ValueSource valueSource, String typeName, Map<String, String> params) {
        fieldDefinitions.add(new FieldDefinition(name, valueExpression,
                valueSource == null ? IndexerConf.DEFAULT_VALUE_SOURCE : valueSource,
                typeName == null ? IndexerConf.DEFAULT_FIELD_TYPE : typeName, params));
        return this;
    }
    
    public IndexerConfBuilder addDocumentExtractDefinition(String prefix, String valueExpression,
            ValueSource valueSource, String type, Map<String, String> params) {
        documentExtractDefinitions.add(new DocumentExtractDefinition(prefix, valueExpression,
                valueSource == null ? IndexerConf.DEFAULT_VALUE_SOURCE : valueSource,
                type == null ? IndexerConf.DEFAULT_EXTRACT_TYPE : type, params));
        return this;
    }

    public IndexerConf build() {
        checkNotNull(table, "table name");
        IndexerConf conf = new IndexerConf(table);
        conf.setMappingType(mappingType != null ? mappingType : IndexerConf.DEFAULT_MAPPING_TYPE);
        conf.setRowReadMode(rowReadMode != null ? rowReadMode : IndexerConf.DEFAULT_ROW_READ_MODE);
        conf.setUniqueKeyField(uniqueKeyField != null ? uniqueKeyField : IndexerConf.DEFAULT_UNIQUE_KEY_FIELD);
        conf.setRowField(rowField);
        conf.setColumnFamilyField(columnFamilyField);
        conf.setMapperClass(mapperClass);
        conf.setUniqueKeyFormatterClass(uniqueKeyFormatterClass != null ?
                uniqueKeyFormatterClass : IndexerConf.DEFAULT_UNIQUE_KEY_FORMATTER);
        conf.setFieldDefinitions(fieldDefinitions);
        conf.setDocumentExtractDefinitions(documentExtractDefinitions);
        conf.setGlobalParams(globalParams);
        return conf;
    }
}
