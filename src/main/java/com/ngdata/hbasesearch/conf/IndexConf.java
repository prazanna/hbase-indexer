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
package com.ngdata.hbasesearch.conf;

import com.ngdata.hbasesearch.Indexer;
import com.ngdata.hbasesearch.StringUniqueKeyFormatter;
import com.ngdata.hbasesearch.UniqueKeyFormatter;

import java.util.Collections;
import java.util.List;

/**
 * The configuration for an index, i.e. this defines the behavior of the {@link Indexer} and of the parser/mapper
 * called by the indexer.
 *
 * <p>Instances of IndexConf can be created using {@link IndexConfBuilder} or from XML using
 * {@link XmlIndexConfReader}.</p>
 */
public class IndexConf {
    private String table;
    private MappingType mappingType;
    private RowReadMode rowReadMode;
    private String uniqueKeyField;
    private Class<? extends UniqueKeyFormatter> uniqueKeyFormatterClass;
    private List<FieldDefinition> fieldDefinitions;

    public enum MappingType { ROW, COLUMN }
    public enum RowReadMode { ALWAYS, NEVER }

    public static final MappingType DEFAULT_MAPPING_TYPE = MappingType.ROW;
    public static final RowReadMode DEFAULT_ROW_READ_MODE = RowReadMode.ALWAYS;
    public static final String DEFAULT_UNIQUE_KEY_FIELD = "id";
    public static final Class<? extends UniqueKeyFormatter> DEFAULT_UNIQUE_KEY_FORMATTER = StringUniqueKeyFormatter.class;

    IndexConf(String table) {
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

    void setUniqueKeyFormatterClass(Class<? extends UniqueKeyFormatter> uniqueKeyFormatterClass) {
        this.uniqueKeyFormatterClass = uniqueKeyFormatterClass;
    }
}
