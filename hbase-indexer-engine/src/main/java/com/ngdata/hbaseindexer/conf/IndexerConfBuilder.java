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
import static com.ngdata.hbaseindexer.conf.FieldDefinition.ValueSource;

import java.util.List;

import com.google.common.collect.Lists;
import com.ngdata.hbaseindexer.uniquekey.UniqueKeyFormatter;

/**
 * A builder for creating {@link IndexerConf} instances.
 */
public class IndexerConfBuilder {
    private String table;
    private String uniqueKeyField = "id";
    private Class<? extends UniqueKeyFormatter> uniqueKeyFormatterClass;
    private IndexerConf.RowReadMode rowReadMode = IndexerConf.RowReadMode.DYNAMIC;
    private IndexerConf.MappingType mappingType = IndexerConf.MappingType.ROW;
    private List<FieldDefinition> fieldDefinitions = Lists.newArrayList();
    private List<DocumentExtractDefinition> documentExtractDefinitions = Lists.newArrayList();

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

    public IndexerConfBuilder uniqueKeyFormatterClass(Class<? extends UniqueKeyFormatter> uniqueKeyFormatterClass) {
        this.uniqueKeyFormatterClass = uniqueKeyFormatterClass;
        return this;
    }

    public IndexerConfBuilder addFieldDefinition(String name, String valueExpression,
            ValueSource valueSource, String typeName) {
        fieldDefinitions.add(new FieldDefinition(name, valueExpression,
                valueSource == null ? IndexerConf.DEFAULT_VALUE_SOURCE : valueSource,
                typeName == null ? IndexerConf.DEFAULT_FIELD_TYPE : typeName));
        return this;
    }
    
    public IndexerConfBuilder addDocumentExtractDefinition(String prefix, String valueExpression,
            ValueSource valueSource, String type) {
        documentExtractDefinitions.add(new DocumentExtractDefinition(prefix, valueExpression,
                valueSource == null ? IndexerConf.DEFAULT_VALUE_SOURCE : valueSource,
                type == null ? IndexerConf.DEFAULT_EXTRACT_TYPE : type));
        return this;
    }

    public IndexerConf build() {
        checkNotNull(table, "table name");
        IndexerConf conf = new IndexerConf(table);
        conf.setMappingType(mappingType != null ? mappingType : IndexerConf.DEFAULT_MAPPING_TYPE);
        conf.setRowReadMode(rowReadMode != null ? rowReadMode : IndexerConf.DEFAULT_ROW_READ_MODE);
        conf.setUniqueKeyField(uniqueKeyField != null ? uniqueKeyField : IndexerConf.DEFAULT_UNIQUE_KEY_FIELD);
        conf.setUniqueKeyFormatterClass(uniqueKeyFormatterClass != null ?
                uniqueKeyFormatterClass : IndexerConf.DEFAULT_UNIQUE_KEY_FORMATTER);
        conf.setFieldDefinitions(fieldDefinitions);
        conf.setDocumentExtractDefinitions(documentExtractDefinitions);
        return conf;
    }
}
