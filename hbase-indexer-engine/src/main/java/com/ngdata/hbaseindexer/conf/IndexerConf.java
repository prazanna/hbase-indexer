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

import com.ngdata.hbaseindexer.indexer.Indexer;
import com.ngdata.hbaseindexer.uniquekey.StringUniqueKeyFormatter;
import com.ngdata.hbaseindexer.uniquekey.UniqueKeyFormatter;

import java.util.Collections;
import java.util.List;

/**
 * The configuration for an indexer, i.e. this defines the behavior of the {@link Indexer} and of the parser/mapper
 * called by the indexer.
 *
 * <p>Instances of IndexerConf can be created using {@link IndexerConfBuilder} or from XML using
 * {@link XmlIndexerConfReader}.</p>
 */
public class IndexerConf {
    private String table;
    private MappingType mappingType;
    private RowReadMode rowReadMode;
    private String uniqueKeyField;
    private Class<? extends UniqueKeyFormatter> uniqueKeyFormatterClass;
    private List<FieldDefinition> fieldDefinitions;
    private List<DocumentExtractDefinition> extractDefinitions;

    public enum MappingType { ROW, COLUMN }

    /**
     * Mode for deciding if a row should be re-read after a mutation event occurs on it. This mode setting is only
     * applicable for row-based indexing.
     */
    public enum RowReadMode {

        /**
         * Re-read a row to be indexed after a mutation event has occurred on the row if the mutation event itself does
         * not include sufficient information to perform indexing.
         */
        DYNAMIC,

        /**
         * Never re-read a row to be indexed after a mutation event.
         */
        NEVER
    }

    public static final MappingType DEFAULT_MAPPING_TYPE = MappingType.ROW;
    public static final RowReadMode DEFAULT_ROW_READ_MODE = RowReadMode.DYNAMIC;
    public static final String DEFAULT_UNIQUE_KEY_FIELD = "id";
    public static final Class<? extends UniqueKeyFormatter> DEFAULT_UNIQUE_KEY_FORMATTER = StringUniqueKeyFormatter.class;
    public static final ValueSource DEFAULT_VALUE_SOURCE = ValueSource.VALUE;
    public static final String DEFAULT_FIELD_TYPE = "string";
    public static final String DEFAULT_EXTRACT_TYPE = "application/octet-stream";

    IndexerConf(String table) {
        this.table = table;
    }

    public String getTable() {
        return table;
    }

    public MappingType getMappingType() {
        return mappingType;
    }

    public RowReadMode getRowReadMode() {
        return rowReadMode;
    }

    public String getUniqueKeyField() {
        return uniqueKeyField;
    }

    public Class<? extends UniqueKeyFormatter> getUniqueKeyFormatterClass() {
        return uniqueKeyFormatterClass;
    }

    public List<FieldDefinition> getFieldDefinitions() {
        return fieldDefinitions;
    }
    
    public List<DocumentExtractDefinition> getDocumentExtractDefinitions() {
        return extractDefinitions;
    }

    void setMappingType(MappingType mappingType) {
        this.mappingType = mappingType;
    }

    void setRowReadMode(RowReadMode rowReadMode) {
        this.rowReadMode = rowReadMode;
    }

    void setUniqueKeyField(String uniqueKeyField) {
        this.uniqueKeyField = uniqueKeyField;
    }

    void setFieldDefinitions(List<FieldDefinition> fieldDefinitions) {
        this.fieldDefinitions = Collections.unmodifiableList(fieldDefinitions);
    }
    
    void setDocumentExtractDefinitions(List<DocumentExtractDefinition> extractDefinitions) {
        this.extractDefinitions = Collections.unmodifiableList(extractDefinitions);
    }

    void setUniqueKeyFormatterClass(Class<? extends UniqueKeyFormatter> uniqueKeyFormatterClass) {
        this.uniqueKeyFormatterClass = uniqueKeyFormatterClass;
    }

   
}
