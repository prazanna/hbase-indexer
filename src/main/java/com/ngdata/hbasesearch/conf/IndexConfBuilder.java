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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import com.google.common.collect.Lists;
import com.ngdata.hbasesearch.UniqueKeyFormatter;

/**
 * A builder for creating {@link IndexConf} instances.
 */
public class IndexConfBuilder {
    private String table;
    private String uniqueKeyField = "id";
    private Class<? extends UniqueKeyFormatter> uniqueKeyFormatterClass;
    private IndexConf.RowReadMode rowReadMode = IndexConf.RowReadMode.ALWAYS;
    private IndexConf.MappingType mappingType = IndexConf.MappingType.ROW;
    private List<FieldDefinition> fieldDefinitions = Lists.newArrayList();

    public IndexConfBuilder table(String table) {
        this.table = table;
        return this;
    }

    public IndexConfBuilder mappingType(IndexConf.MappingType mappingType) {
        this.mappingType = mappingType;
        return this;
    }

    public IndexConfBuilder rowReadMode(IndexConf.RowReadMode rowReadMode) {
        this.rowReadMode = rowReadMode;
        return this;
    }

    public IndexConfBuilder uniqueyKeyField(String uniqueKeyField) {
        this.uniqueKeyField = uniqueKeyField;
        return this;
    }

    public IndexConfBuilder uniqueKeyFormatterClass(Class<? extends UniqueKeyFormatter> uniqueKeyFormatterClass) {
        this.uniqueKeyFormatterClass = uniqueKeyFormatterClass;
        return this;
    }

    public IndexConfBuilder addFieldDefinition(String name, String valueExpression,
            ValueSource valueSource, String typeName) {
        fieldDefinitions.add(new FieldDefinition(name, valueExpression, valueSource, typeName));
        return this;
    }

    public IndexConf create() {
        checkNotNull(table, "table name");
        IndexConf conf = new IndexConf(table);
        conf.setMappingType(mappingType != null ? mappingType : IndexConf.DEFAULT_MAPPING_TYPE);
        conf.setRowReadMode(rowReadMode != null ? rowReadMode : IndexConf.DEFAULT_ROW_READ_MODE);
        conf.setUniqueKeyField(uniqueKeyField != null ? uniqueKeyField : IndexConf.DEFAULT_UNIQUE_KEY_FIELD);
        conf.setUniqueKeyFormatterClass(uniqueKeyFormatterClass != null ?
                uniqueKeyFormatterClass : IndexConf.DEFAULT_UNIQUE_KEY_FORMATTER);
        conf.setFieldDefinitions(fieldDefinitions);
        return conf;
    }
}
